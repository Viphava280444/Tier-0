"""
_InsertReprocessingRequest_

Oracle implementation of InsertReprocessingRequest

Insert a new reprocessing request into the reprocessing_request table.
"""

import time

from WMCore.Database.DBFormatter import DBFormatter

class InsertReprocessingRequest(DBFormatter):

    def execute(self, binds, conn = None, transaction = False):

        sql = """INSERT INTO reprocessing_request
                 (INPUT_DATASET, RUN_ID, MIN_LUMI, MAX_LUMI,
                  GLOBAL_TAG, CMSSW_VERSION, SCRAM_ARCH, PROC_VERSION, SCENARIO,
                  RECO_SPLIT, MULTICORE,
                  WRITE_RECO, WRITE_AOD, WRITE_MINIAOD, WRITE_NANOAOD, WRITE_DQM,
                  ALCA_SKIM, PHYSICS_SKIM, DQM_SEQ, NANO_FLAVOUR,
                  STATUS, CREATED_AT, UPDATED_AT)
                 VALUES (:INPUT_DATASET, :RUN_ID, :MIN_LUMI, :MAX_LUMI,
                         :GLOBAL_TAG, :CMSSW_VERSION, :SCRAM_ARCH, :PROC_VERSION, :SCENARIO,
                         :RECO_SPLIT, :MULTICORE,
                         :WRITE_RECO, :WRITE_AOD, :WRITE_MINIAOD, :WRITE_NANOAOD, :WRITE_DQM,
                         :ALCA_SKIM, :PHYSICS_SKIM, :DQM_SEQ, :NANO_FLAVOUR,
                         'SUBMITTED', :NOW, :NOW)
                 """

        now = int(time.time())
        if isinstance(binds, list):
            for b in binds:
                b['NOW'] = now
        else:
            binds['NOW'] = now

        self.dbi.processData(sql, binds, conn = conn,
                             transaction = transaction)

        return
