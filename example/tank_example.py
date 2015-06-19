#!/usr/bin/python2
import yaml
import logging
logger = logging.getLogger(__name__)

import trollius as asyncio

from trollius import From
try:
    from futures import CancelledError
except ImportError:
    from concurrent.futures import CancelledError

import yandex_tank_api_client.async as tankapi


def run_as_main(coro):
    """
    This function should be used to launch the top-level
    coroutine if you use launch_subtask.
    It waits for all pending tasks
    after the top-level coroutine exits.
    """

    task = asyncio.async(_main_wrapper(coro))
    task.add_done_callback(_main_task_done)
    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()
        return
    except KeyboardInterrupt:
        logger.info("Interrupted")
    except Exception:
        logger.exception("Exception in event loop:")
    except:
        logger.exception("Something starnge caught in event loop")
    logger.warning("Cancelling main task")
    task.cancel()
    loop.run_forever()


@asyncio.coroutine
def _main_wrapper(coro):
    try:
        yield From(coro)
    except CancelledError:
        raise
    except BaseException:
        logger.exception("Exception in main task")
        raise
    except:
        logger.error("Main task raised something strange")
        raise


def _main_task_done(main_task):
    """Callback to be called when the top-level coroutine dies"""
    if main_task.cancelled():
        logger.info("Main task cancelled")
    else:
        ex = main_task.exception()
        if ex is None:
            logger.info("Main task finished")
        else:
            logger.error("Main task failed")
    logger.info("Finding remaining tasks...")
    pending = set(task for task in asyncio.Task.all_tasks() if not task.done())
    logger.info("Waiting for %s remaining tasks to terminate", len(pending))
    gth = asyncio.gather(*pending, return_exceptions=True)
    gth.add_done_callback(_stop_loop)


def _stop_loop(_):
    """Callback to be called after all pending tasks stop"""
    logger.info("Stopping event loop")
    asyncio.get_event_loop().stop()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    cfg = yaml.safe_load(open('load.yaml').read())
    logging.info("Config:\n%s",yaml.safe_dump(cfg[0]))
    tsk = tankapi.shoot(*cfg)
    run_as_main(tsk)
