"""
Yandex.Tank API coroutine client capable of multi-tank multi-config tests.

Note: HTTP requests issued by this module are blocking
(usually they are small and are processed quickly).
"""
import time
import fnmatch
import os.path
import logging
import urllib2
import yaml
logger = logging.getLogger(__name__)

from trollius import coroutine, sleep, Return,\
    CancelledError, From, gather, async

import yandex_tank_api_client.session as tankapi


@coroutine
def shoot(*cfgs):
    """
    Performs multi-tank multi-config test.
    Accepts one or more config dicts.
    Returns list of test ID's.
    Raises TankLocked and TestFailed.
    """
    try:
        sessions = [Session(**cfg) for cfg in cfgs]#pylint: disable=W0142
    except Exception:
        logger.exception("Failed to initialize session objects, config:\n%s",
                         yaml.safe_dump(cfgs))
        raise
    prepares = []
    runs = []
    stops = []
    try:
        try:
            prepares = [async(session.prepare()) for session in sessions]
            yield From(gather(*prepares)) #pylint: disable=W0142
            logger.info("All tanks are prepared. STARTING TO SHOOT.")
            runs = [async(session.run()) for session in sessions]
            yield From(gather(*runs))#pylint: disable=W0142
        except KeyboardInterrupt:
            logger.info("Test interrupted")
            raise
        except CancelledError:
            logger.info("Test cancelled")
            raise
        except Exception:
            logger.exception("Exception occured in Test.run()")
            raise
        except BaseException:
            logger.exception("Something strange caught by Test.run()")
            raise
    except BaseException as ex:
        logger.info("Stopping remaining tank sessions...")
        stops = [async(session.stop()) for session in sessions]
        yield From(gather(*stops, return_exceptions=True))#pylint: disable=W0142
        raise ex
    finally:
        for task in prepares + runs + stops:
            task.cancel()
    logger.info("All tanks are done.")
    raise Return([
        session.session.s_id
        if session.session is not None else None
        for session in sessions
    ])


class TankLocked(RuntimeError):

    """Raised when another session is running or tank lock is present"""
    pass


class TestFailed(RuntimeError):

    """Raised when test status indicates an irrecoverable failure"""

    def __init__(self, status):
        failures = '\n'.join(
            ("================\n"
                "Stage: %s\n"
                "Reason:\n%s\n") %
            (flr.get('stage', '-'), flr.get('reason', '-'))
            for flr in status.get('failures', [])
        )
        msg = ("Test failed:\n"
               "retcode=%s\n"
               "test=%s\n"
               "Failures:\n%s\n"
               "===============\n") % (status.get('retcode', '-'),
                                       status.get('test', '-'),
                                       failures)
        RuntimeError.__init__(self, msg)
        self.status = status


class Session(object):

    """
    run():
        starts test, downloads artifacts and waits for finish
    stop():
        stops test at arbitratry point
    """

    def __init__(self, **params):
        self.session = None
        log_name = params.get('log_name', params.get(
            'options', {}).get('meta.job_name', __name__))
        self.log = logging.getLogger(log_name)
        try:

            self.tanks = params.get('tanks', [])
            if 'tank' in params:
                self.tanks.append(params['tank'])
		
            options = []
            for key, value in params.get('options',{}).iteritems():
                if value is None:
                    value = ''
                options.append((key, value))

            if 'config' in params and params['config']:
                if isinstance(params['config'], str):
                    config_files = [params['config']]
                elif isinstance(params['config'], list):
                    config_files = params['config']
                else:
                    raise ValueError("Bad config entry")
            self.tank_config = '\n\n'.join(
                open(cnf).read() for cnf in config_files)
            self.tank_config += tankapi.make_ini(options)

            self.download_list = params.get('download', [])
            self.upload = []
            for entry in params.get('upload', []):
                if isinstance(entry, str):
                    _, target_file = os.path.split(entry)
                    self.upload.append((entry, target_file))
                else:
                    try:
		        if len(entry) != 2:
		            raise ValueError("")
                    except:
                        raise ValueError("Malformed upload section: " + str(entry))
                    else:
                        self.upload.append(entry)
	
            self.start_timeout = params.get('start_timeout', 14400)
            self.expected_codes = params.get('expected_codes', [0])
	    self.artifacts_by_session = params.get('artifacts_by_session', False)
        except Exception:
            self.log.exception("Failed to initialize Session object")
            raise
        try:
            _ = [int(code) for code in self.expected_codes]
        except ValueError:
            raise ValueError(
                'expected_codes should be an iterable of INTEGERS')
        except TypeError:
            raise ValueError(
                'expected_codes should be an ITERABLE of integers')

    @coroutine
    def prepare(self):
        """
        Obtain tank session on some tank
        Return when it passes 'prepare' stage
        """
        start_time = time.time()
        while True:
            for tank in self.tanks:
                try:
                    yield From(self._prepare_tank(tank))
                except tankapi.RetryLater:
                    self.log.info("%s is either absent or locked", tank)
                    continue
                self.log.info("Tank %s is ready to start test", tank)
                raise Return()
            wait_time = time.time() - start_time
            if wait_time > self.start_timeout:
                raise TankLocked(
                    "Tank locked, failed to start test in %d seconds" %
                    wait_time)
            self.log.info("All allowed tanks are locked, waiting 5 seconds...")
            yield From(sleep(5))

    @coroutine
    def run(self):
        """Runs single test"""
        self.session.set_breakpoint("unlock")
        yield From(self._run_until_stage_completion('postprocess'))
        yield From(self._finish())

    @coroutine
    def stop(self, wait=True):
        """
        Stops current session, if any.
        Catches all exceptions from tankapi client.
        """
        if self.session is None:
            self.log.info("No session to stop")
            raise Return()
        n_stop_attempts = 0
        while True:
            self.log.info("Asking tank to stop session %s", self.session.s_id)
            try:
                self.session.stop()
            except tankapi.NothingDone:
                self.log.info("Session %s is not running", self.session.s_id)
            except urllib2.URLError:
                self.log.exception(
                    "Failed to communicate with %s to stop session",
                    self.session.tank)
                n_stop_attempts += 1
                if n_stop_attempts < 5:
                    yield From(sleep(5))
                else:
                    raise
            except:
                self.log.critical("Failed to stop session %s at tank %s",
                                  self.session.s_id, self.session.tank,
                                  exc_info=True)
                raise
            break

        if wait:
            yield From(self._finish())

    @coroutine
    def _prepare_tank(self, tank):
        """
        Return tankapi.Session for acquired tank
        Raises tankapi.RetryLater if tank is busy
        Should not be called after some session was successfully acquired
        """
	first_break = 'configure' if self.upload else 'start'
        self.log.info("Trying to start session at %s ...", tank)
        try:
            self.session = tankapi.Session(
                tank=tank,
                config_contents=self.tank_config,
                stage=first_break
            )
        except urllib2.URLError as exc:
            self.log.exception("Failed to communicate with %s", tank)
            raise tankapi.RetryLater(str(exc), {})
        self.log.info("Started session %s",self.session.s_id)
        if self.upload:
            yield From(self._run_until_stage_completion('init'))
            for local_path, remote_name in self.upload:
                self.session.upload(local_path, remote_name)
            self.session.set_breakpoint('start')	 
        yield From(self._run_until_stage_completion('prepare'))

    @coroutine
    def _finish(self):
        """
        Download artifacts and finalize session (if any)
        """
        try:
            self._download_artifacts()
            try:
                self.session.set_breakpoint('finished')
            except tankapi.APIError as api_err:
                if api_err.get('status', '--unknown--')\
                        not in ('success', 'failed'):
                    raise
            status = yield From(
                self._run_until_stage_completion()
            )
            if status['status'] == 'success' and\
                    (status['retcode'] is not None and
                     int(status['retcode']) in self.expected_codes):
                self.log.info("Test succeded")
                raise Return()
        except tankapi.APIError:
            self.log.exception(
                "Failed to finalize session %s", self.session.s_id)

        self.log.info("Test failed")
        raise TestFailed(status)

    def _download_artifacts(self):
        """Downloads files by mask into specified dir"""
	if self.artifacts_by_session:
	    artifact_dir = self.session.s_id
	    try:
	        os.makedirs(self.session.s_id)
	    except OSError:
	        self.log.exception("Failed to create artifact directory %s",
				       self.session.s_id)
	    return
	else:
            artifact_dir = '.'
        try:
            artifacts = self.session.get_artifact_list()
        except tankapi.APIError:
            self.log.exception("Failed to obtain artifact list")
            return
        except urllib2.URLError:
            self.log.exception("Failed to obtain artifact list")
            return

        for art in artifacts:
            if any(fnmatch.fnmatch(art, patt) for patt in self.download_list):
                try:
                    self.log.info("Downloading %s from %s",
                                  art, self.session.tank)
                    self.session.download_artifact(
                        art, os.path.join(artifact_dir, art))
                except urllib2.URLError:
                    self.log.exception(
                        "Failed to download %s from %s", art, self.session.tank)
                except tankapi.APIError:
                    self.log.exception(
                        "Failed to download %s from %s", art, self.session.tank)

    @coroutine
    def _run_until_stage_completion(
            self,
            target_stage=None,
            poll_interval=5,
            poll_failure_limit=6
    ):
        """
        Waits either for test success or for completion of a patrticular stage.
        Returns status if successful
        Raises:
            tankapi.RetryLater if tank lock is found
            TestFailed
        """
        poll_failure_count = 0
        status = None
        while poll_failure_count < poll_failure_limit:
            try:
                status = self.session.get_status()
            except urllib2.URLError as err:
                self.log.warning(
                    "Failed to obtain session status: %s", str(err))
                poll_failure_count += 1
            else:
                poll_failure_count = 0
                if 'failures' in status and \
                        any(flr['stage'] == 'lock'\
                            for flr in status['failures']):
                    self.log.info("%s is locked", self.session.tank)
                    raise tankapi.RetryLater()

                if status['status'] == 'failed':
                    self.log.warning(
                        "Session %s failed:\n%s",
                        self.session.s_id,
                        yaml.safe_dump(status.get('failures', {}))
                    )
                    raise TestFailed(status)

                if status['status'] == 'success':
                    self.log.info("Session %s finished successfully",
                                  self.session.s_id)
                    raise Return(status)

                last_stage = status.get('current_stage', 'unknown')
                completed = status.get('stage_completed', False)
                self.log.info(
                    "Session %s: %s, %scomplete",
                    self.session.s_id,
                    last_stage,
                    '' if completed else 'in'
                )
                if target_stage == last_stage and completed:
                    raise Return(status)
            yield From(sleep(poll_interval))
        self.log.warning("Exceeded poll failure limit")
        if status is None or status['stage'] == 'lock':
            # We have not locked the tank yet
            raise tankapi.RetryLater()
        # We have locked the tank and it died quietly
        raise RuntimeError("Tank poll failure limit exceeded")
