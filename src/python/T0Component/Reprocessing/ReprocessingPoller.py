#!/usr/bin/env python
"""
_ReprocessingPoller_

The actual work done by the Reprocessing component.

Drives the reprocessing-from-RAW state machine:
  SUBMITTED -> VALIDATED -> STAGING/READY -> STAGED -> INJECTED -> DONE/FAILED

Each poll cycle advances eligible requests by one state transition.
"""

import logging
import time
import threading

from Utils.Timers import timeFunction
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.DAOFactory import DAOFactory
from WMCore.Configuration import loadConfigurationFile
from WMCore.Services.DBS.DBS3Reader import DBS3Reader
from WMCore.WMSpec.StdSpecs.PromptReco import PromptRecoWorkloadFactory
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMBS.DataStructs.Fileset import Fileset

from T0.Reprocessing import DataStagingAPI


REQUEST_START_STATE = "new"


class ReprocessingPoller(BaseWorkerThread):

    def __init__(self, config):
        """
        _init_

        Set up DAOs, DBS reader, and Rucio client.
        """
        BaseWorkerThread.__init__(self)

        myThread = threading.currentThread()

        self.daoFactory = DAOFactory(package = "T0.WMBS",
                                     logger = logging,
                                     dbinterface = myThread.dbi)

        self.reprocessingConfigFile = config.Reprocessing.reprocessingConfigFile
        self.specDirectory = config.Reprocessing.specDirectory
        self.processingSiteRSE = getattr(config.Reprocessing, "processingSiteRSE", "T0_CH_CERN_Disk")
        self.stagingTimeout = getattr(config.Reprocessing, "stagingTimeout", 48 * 3600)
        self.stagingLifetime = getattr(config.Reprocessing, "stagingLifetime", 7 * 86400)
        self.rucioAccount = getattr(config.Reprocessing, "rucioAccount", "tier0_prod")

        self.dqmUploadProxy = getattr(config.Reprocessing, "dqmUploadProxy", None)

        # DBS reader setup
        dbsUrl = getattr(config.Reprocessing, "dbsUrl",
                         "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader")
        self.dbsReader = DBS3Reader(dbsUrl)

        # Rucio client - initialized lazily on first use
        self._rucioClient = None

        # Track which config requests have been inserted to DB
        self.insertedRequests = set()

        return

    @property
    def rucioClient(self):
        """Lazy initialization of Rucio client."""
        if self._rucioClient is None:
            from rucio.client import Client
            self._rucioClient = Client(account=self.rucioAccount)
        return self._rucioClient

    @timeFunction
    def algorithm(self, parameters = None):
        """
        _algorithm_

        Main polling loop. Load config, insert new requests,
        then advance all active requests by one state.
        """
        logging.debug("Running ReprocessingPoller algorithm...")
        myThread = threading.currentThread()

        # Load reprocessing configuration
        reprocessingConfig = None
        try:
            reprocessingConfig = loadConfigurationFile(self.reprocessingConfigFile)
        except Exception:
            logging.exception("Cannot load reprocessing configuration file")

        # Insert new requests from config into DB
        if reprocessingConfig is not None:
            self.insertConfigRequests(reprocessingConfig)

        # Process active requests by status
        try:
            self.processSubmitted()
        except Exception:
            logging.exception("Error in processSubmitted()")

        try:
            self.processValidated()
        except Exception:
            logging.exception("Error in processValidated()")

        try:
            self.processStaging()
        except Exception:
            logging.exception("Error in processStaging()")

        try:
            self.processReady()
        except Exception:
            logging.exception("Error in processReady()")

        try:
            self.processInjected()
        except Exception:
            logging.exception("Error in processInjected()")

        return

    def insertConfigRequests(self, reprocessingConfig):
        """
        _insertConfigRequests_

        Read reprocessing requests from the config file and insert
        any new ones into the database.
        """
        if not hasattr(reprocessingConfig, 'Global') or \
           not hasattr(reprocessingConfig.Global, 'ReprocessingRequests'):
            return

        insertDAO = self.daoFactory(classname = "Reprocessing.InsertReprocessingRequest")
        myThread = threading.currentThread()

        for idx, request in enumerate(reprocessingConfig.Global.ReprocessingRequests):
            requestKey = (request.get('InputDataset', ''),
                          request.get('ProcVersion', 0))

            if requestKey in self.insertedRequests:
                continue

            # Validate required fields
            requiredFields = ['InputDataset', 'GlobalTag', 'CMSSWVersion',
                              'ScramArch', 'ProcVersion', 'Scenario']
            missing = [f for f in requiredFields if f not in request or request[f] is None]
            if missing:
                logging.error("Reprocessing request %d missing required fields: %s",
                              idx, ", ".join(missing))
                continue

            # Validate RAW tier
            inputDataset = request['InputDataset']
            if not inputDataset.endswith('/RAW'):
                logging.error("Reprocessing request %d: input dataset must be RAW tier, got %s",
                              idx, inputDataset)
                continue

            binds = {
                'INPUT_DATASET': inputDataset,
                'RUN_ID': request.get('RunId', None),
                'MIN_LUMI': request.get('MinLumi', None),
                'MAX_LUMI': request.get('MaxLumi', None),
                'GLOBAL_TAG': request['GlobalTag'],
                'CMSSW_VERSION': request['CMSSWVersion'],
                'SCRAM_ARCH': request['ScramArch'],
                'PROC_VERSION': request['ProcVersion'],
                'SCENARIO': request['Scenario'],
                'RECO_SPLIT': request.get('RecoSplit', 500),
                'MULTICORE': request.get('Multicore', 8),
                'WRITE_RECO': 1 if 'RECO' in request.get('WriteTiers', []) else 0,
                'WRITE_AOD': 1 if 'AOD' in request.get('WriteTiers', ['AOD', 'MINIAOD', 'DQM']) else 0,
                'WRITE_MINIAOD': 1 if 'MINIAOD' in request.get('WriteTiers', ['AOD', 'MINIAOD', 'DQM']) else 0,
                'WRITE_NANOAOD': 1 if 'NANOAOD' in request.get('WriteTiers', []) else 0,
                'WRITE_DQM': 1 if 'DQM' in request.get('WriteTiers', ['AOD', 'MINIAOD', 'DQM']) else 0,
                'ALCA_SKIM': ",".join(request.get('AlcaSkims', [])) or None,
                'PHYSICS_SKIM': ",".join(request.get('PhysicsSkims', [])) or None,
                'DQM_SEQ': ",".join(request.get('DqmSequences', [])) or None,
                'NANO_FLAVOUR': request.get('NanoFlavour', None),
            }

            try:
                myThread.transaction.begin()
                insertDAO.execute(binds, conn = myThread.transaction.conn,
                                  transaction = True)
                myThread.transaction.commit()
                self.insertedRequests.add(requestKey)
                logging.info("Inserted reprocessing request for %s (v%d)",
                             inputDataset, request['ProcVersion'])
            except Exception as ex:
                myThread.transaction.rollback()
                logging.error("Failed to insert reprocessing request for %s: %s",
                              inputDataset, str(ex))

        return

    def processSubmitted(self):
        """
        _processSubmitted_

        SUBMITTED -> VALIDATED:
        Verify input dataset exists in DBS, retrieve file count/size.
        """
        getByStatusDAO = self.daoFactory(classname = "Reprocessing.GetReprocessingByStatus")
        updateStatusDAO = self.daoFactory(classname = "Reprocessing.UpdateReprocessingStatus")
        myThread = threading.currentThread()

        requests = getByStatusDAO.execute(status = "SUBMITTED",
                                          conn = myThread.transaction.conn,
                                          transaction = False)

        for request in requests:
            requestId = request['id']
            inputDataset = request['input_dataset']

            try:
                filesByBlock = DataStagingAPI.discoverRAWFiles(
                    self.dbsReader,
                    inputDataset,
                    runId = request['run_id'],
                    minLumi = request['min_lumi'],
                    maxLumi = request['max_lumi']
                )

                totalFiles = sum(len(v) for v in filesByBlock.values())
                totalSize = sum(f['size'] for files in filesByBlock.values() for f in files)
                logging.info("Request %d: validated %s - %d files, %.2f TB",
                             requestId, inputDataset, totalFiles,
                             totalSize / (1024**4))

                myThread.transaction.begin()
                updateStatusDAO.execute(requestId, "VALIDATED",
                                        conn = myThread.transaction.conn,
                                        transaction = True)
                myThread.transaction.commit()

            except Exception as ex:
                myThread.transaction.begin()
                updateStatusDAO.execute(requestId, "FAILED",
                                        errorMessage = str(ex),
                                        conn = myThread.transaction.conn,
                                        transaction = True)
                myThread.transaction.commit()
                logging.error("Request %d failed validation: %s", requestId, str(ex))

        return

    def processValidated(self):
        """
        _processValidated_

        VALIDATED -> READY or STAGING:
        Classify file locations, create staging rules if needed.
        """
        getByStatusDAO = self.daoFactory(classname = "Reprocessing.GetReprocessingByStatus")
        updateStatusDAO = self.daoFactory(classname = "Reprocessing.UpdateReprocessingStatus")
        myThread = threading.currentThread()

        requests = getByStatusDAO.execute(status = "VALIDATED",
                                          conn = myThread.transaction.conn,
                                          transaction = False)

        for request in requests:
            requestId = request['id']
            inputDataset = request['input_dataset']

            try:
                filesByBlock = DataStagingAPI.discoverRAWFiles(
                    self.dbsReader,
                    inputDataset,
                    runId = request['run_id'],
                    minLumi = request['min_lumi'],
                    maxLumi = request['max_lumi']
                )

                locations = DataStagingAPI.classifyFileLocations(
                    self.rucioClient, filesByBlock, self.processingSiteRSE
                )

                if locations['missing']:
                    raise RuntimeError("Missing files with no replicas: %s" %
                                       ", ".join(locations['missing'][:10]))

                myThread.transaction.begin()

                if locations['on_tape']:
                    # Need staging
                    ruleIds = DataStagingAPI.createStagingRules(
                        self.rucioClient,
                        locations['on_tape'],
                        self.processingSiteRSE,
                        lifetime = self.stagingLifetime,
                        account = self.rucioAccount
                    )
                    ruleIdStr = ",".join(ruleIds)
                    updateStatusDAO.execute(requestId, "STAGING",
                                            rucioRuleId = ruleIdStr,
                                            conn = myThread.transaction.conn,
                                            transaction = True)
                    logging.info("Request %d: staging %d blocks from tape",
                                 requestId, len(locations['on_tape']))
                else:
                    # All on disk
                    updateStatusDAO.execute(requestId, "READY",
                                            conn = myThread.transaction.conn,
                                            transaction = True)
                    logging.info("Request %d: all data on disk, ready for processing",
                                 requestId)

                myThread.transaction.commit()

            except Exception as ex:
                myThread.transaction.begin()
                updateStatusDAO.execute(requestId, "FAILED",
                                        errorMessage = str(ex),
                                        conn = myThread.transaction.conn,
                                        transaction = True)
                myThread.transaction.commit()
                logging.error("Request %d failed location check: %s", requestId, str(ex))

        return

    def processStaging(self):
        """
        _processStaging_

        STAGING -> STAGED or FAILED:
        Check Rucio rule status, detect timeout.
        """
        getByStatusDAO = self.daoFactory(classname = "Reprocessing.GetReprocessingByStatus")
        updateStatusDAO = self.daoFactory(classname = "Reprocessing.UpdateReprocessingStatus")
        myThread = threading.currentThread()

        requests = getByStatusDAO.execute(status = "STAGING",
                                          conn = myThread.transaction.conn,
                                          transaction = False)

        for request in requests:
            requestId = request['id']
            ruleIdStr = request['rucio_rule_id']

            if not ruleIdStr:
                continue

            ruleIds = ruleIdStr.split(",")

            try:
                stagingStatus = DataStagingAPI.checkStagingStatus(
                    self.rucioClient, ruleIds
                )

                logging.info("Request %d staging: %s (%d/%d rules OK, %d/%d locks OK)",
                             requestId, stagingStatus['status'],
                             stagingStatus['ok_count'], stagingStatus['total_count'],
                             stagingStatus['locks_ok'], stagingStatus['locks_total'])

                myThread.transaction.begin()

                if stagingStatus['status'] == 'OK':
                    updateStatusDAO.execute(requestId, "STAGED",
                                            conn = myThread.transaction.conn,
                                            transaction = True)
                    logging.info("Request %d: staging complete", requestId)

                elif stagingStatus['status'] == 'FAILED':
                    updateStatusDAO.execute(requestId, "FAILED",
                                            errorMessage = "Rucio staging rule failed",
                                            conn = myThread.transaction.conn,
                                            transaction = True)

                elif stagingStatus['status'] == 'STUCK':
                    # Check timeout
                    elapsed = int(time.time()) - request['updated_at']
                    if elapsed > self.stagingTimeout:
                        updateStatusDAO.execute(requestId, "FAILED",
                                                errorMessage = "Staging timeout after %d seconds" % elapsed,
                                                conn = myThread.transaction.conn,
                                                transaction = True)
                        logging.error("Request %d: staging timeout", requestId)
                    else:
                        logging.warning("Request %d: staging stuck for %d seconds (timeout: %d)",
                                        requestId, elapsed, self.stagingTimeout)

                myThread.transaction.commit()

            except Exception as ex:
                logging.error("Request %d staging check error: %s", requestId, str(ex))

        return

    def processReady(self):
        """
        _processReady_

        READY/STAGED -> INJECTED:
        Build specArguments, create PromptReco workflow, inject to WMAgent.
        """
        updateStatusDAO = self.daoFactory(classname = "Reprocessing.UpdateReprocessingStatus")
        myThread = threading.currentThread()

        # Process both READY and STAGED requests
        getByStatusDAO = self.daoFactory(classname = "Reprocessing.GetReprocessingByStatus")

        for status in ("READY", "STAGED"):
            requests = getByStatusDAO.execute(status = status,
                                              conn = myThread.transaction.conn,
                                              transaction = False)

            for request in requests:
                requestId = request['id']
                inputDataset = request['input_dataset']

                try:
                    workflowName = "ReReco_Run%s_%s_v%d" % (
                        request['run_id'] if request['run_id'] else "All",
                        inputDataset.split("/")[1],
                        request['proc_version']
                    )

                    # Build writeTiers list
                    writeTiers = []
                    if request['write_reco']:
                        writeTiers.append("RECO")
                    if request['write_aod']:
                        writeTiers.append("AOD")
                    if request['write_miniaod']:
                        writeTiers.append("MINIAOD")
                    if request['write_nanoaod']:
                        writeTiers.append("NANOAOD")
                    if request['write_dqm']:
                        writeTiers.append("DQMIO")

                    alcaSkims = request['alca_skim'].split(",") if request['alca_skim'] else []
                    if alcaSkims:
                        writeTiers.append("ALCARECO")

                    physicsSkims = request['physics_skim'].split(",") if request['physics_skim'] else []
                    dqmSequences = request['dqm_seq'].split(",") if request['dqm_seq'] else []
                    nanoFlavours = [request['nano_flavour']] if request['nano_flavour'] else []

                    # Build specArguments following releasePromptReco pattern
                    specArguments = {}
                    specArguments['TimePerEvent'] = 12
                    specArguments['SizePerEvent'] = 2000

                    multicore = request['multicore'] or 8
                    specArguments['Memory'] = 2300 + (multicore - 1) * 2300
                    specArguments['Multicore'] = multicore

                    specArguments['Requestor'] = "Tier0"
                    specArguments['RequestName'] = workflowName
                    specArguments['RequestString'] = workflowName
                    specArguments['RequestorDN'] = "Tier0"
                    specArguments['RequestDate'] = []
                    specArguments['RequestTransition'] = []
                    specArguments['RequestStatus'] = REQUEST_START_STATE
                    specArguments['RequestPriority'] = 100000
                    specArguments['PriorityTransition'] = []

                    # Extract acquisition era from input dataset path
                    processedName = inputDataset.split("/")[2]
                    acqEra = processedName.split("-")[0]
                    specArguments['AcquisitionEra'] = acqEra

                    specArguments['CMSSWVersion'] = request['cmssw_version']
                    specArguments['ScramArch'] = request['scram_arch']

                    if request['run_id']:
                        specArguments['RunNumber'] = request['run_id']

                    specArguments['SplittingAlgo'] = "EventAwareLumiBased"
                    specArguments['EventsPerJob'] = request['reco_split']

                    specArguments['RobustMerge'] = False

                    specArguments['ProcessingString'] = "PromptReco"
                    specArguments['ProcessingVersion'] = request['proc_version']
                    specArguments['Scenario'] = request['scenario']

                    specArguments['GlobalTag'] = request['global_tag']

                    specArguments['InputDataset'] = inputDataset

                    specArguments['WriteTiers'] = writeTiers
                    specArguments['AlcaSkims'] = alcaSkims
                    specArguments['PhysicsSkims'] = physicsSkims
                    specArguments['DQMSequences'] = dqmSequences
                    specArguments['NanoFlavours'] = nanoFlavours

                    specArguments['UnmergedLFNBase'] = "/store/unmerged/data"
                    specArguments['MergedLFNBase'] = "/store/data"

                    specArguments['ValidStatus'] = "VALID"
                    specArguments['EnableHarvesting'] = "True"

                    if self.dqmUploadProxy:
                        specArguments['DQMUploadProxy'] = self.dqmUploadProxy
                    specArguments['DQMUploadUrl'] = "https://cmsweb.cern.ch/dqm/offline"

                    # Create workflow
                    factory = PromptRecoWorkloadFactory()
                    wmSpec = factory.factoryWorkloadConstruction(workflowName, specArguments)

                    wmSpec.setOwnerDetails("Tier0@cern.ch", "T0",
                                           { 'vogroup': 'DEFAULT', 'vorole': 'DEFAULT',
                                             'dn' : "Tier0@cern.ch" })

                    taskName = "Reco"
                    wmbsHelper = WMBSHelper(wmSpec, taskName,
                                            cachepath = self.specDirectory)

                    myThread.transaction.begin()
                    wmbsHelper.createSubscription(
                        wmSpec.getTask(taskName),
                        Fileset(name = "ReReco_%s" % inputDataset),
                        alternativeFilesetClose = True
                    )
                    updateStatusDAO.execute(requestId, "INJECTED",
                                            workflowName = workflowName,
                                            conn = myThread.transaction.conn,
                                            transaction = True)
                    myThread.transaction.commit()

                    logging.info("Request %d: workflow %s injected to WMAgent",
                                 requestId, workflowName)

                except Exception as ex:
                    myThread.transaction.rollback()
                    myThread.transaction.begin()
                    updateStatusDAO.execute(requestId, "FAILED",
                                            errorMessage = str(ex),
                                            conn = myThread.transaction.conn,
                                            transaction = True)
                    myThread.transaction.commit()
                    logging.error("Request %d workflow creation failed: %s",
                                  requestId, str(ex))

        return

    def processInjected(self):
        """
        _processInjected_

        INJECTED -> DONE or FAILED:
        Query WMAgent workflow completion status.
        """
        getByStatusDAO = self.daoFactory(classname = "Reprocessing.GetReprocessingByStatus")
        updateStatusDAO = self.daoFactory(classname = "Reprocessing.UpdateReprocessingStatus")
        myThread = threading.currentThread()

        requests = getByStatusDAO.execute(status = "INJECTED",
                                          conn = myThread.transaction.conn,
                                          transaction = False)

        for request in requests:
            requestId = request['id']
            workflowName = request['workflow_name']

            if not workflowName:
                continue

            try:
                # Check workflow status by querying WMBS subscriptions
                # A workflow is done when all its subscriptions are complete
                getWorkflowStatusDAO = self.daoFactory(
                    classname = "Tier0Feeder.GetNotClosedOutWorkflows"
                )
                notClosedOut = getWorkflowStatusDAO.execute(
                    conn = myThread.transaction.conn,
                    transaction = False
                )

                # If workflow not in the "not closed out" list, it's done
                workflowDone = True
                for workflow in notClosedOut:
                    if workflowName in str(workflow):
                        workflowDone = False
                        break

                if workflowDone:
                    myThread.transaction.begin()
                    updateStatusDAO.execute(requestId, "DONE",
                                            conn = myThread.transaction.conn,
                                            transaction = True)
                    myThread.transaction.commit()
                    logging.info("Request %d: workflow %s completed",
                                 requestId, workflowName)

            except Exception as ex:
                logging.error("Request %d completion check error: %s",
                              requestId, str(ex))

        return
