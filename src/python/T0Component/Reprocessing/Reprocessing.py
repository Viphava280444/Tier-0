#!/usr/bin/env python
"""
_Reprocessing_

Component wrapper for the ReprocessingPoller.

Handles reprocessing from existing RAW datasets by managing
the lifecycle: validation, data staging, workflow creation,
and completion tracking.
"""

import logging
import threading

from WMCore.Agent.Harness import Harness

from T0Component.Reprocessing.ReprocessingPoller import ReprocessingPoller


class Reprocessing(Harness):
    """
    Component that drives the reprocessing-from-RAW state machine.
    For the actual work, see ReprocessingPoller.
    """

    def __init__(self, config):
        Harness.__init__(self, config)
        self.pollTime = 1

        print("Reprocessing.__init__")

    def preInitialization(self):
        """
        Add the ReprocessingPoller worker thread.
        """
        print("Reprocessing.preInitialization")

        myThread = threading.currentThread()

        pollInterval = self.config.Reprocessing.pollInterval
        logging.info("Setting reprocessing poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(ReprocessingPoller(self.config),
                                               pollInterval)
        return
