"""
_UpdateReprocessingStatus_

Oracle implementation of UpdateReprocessingStatus

Update the status of a reprocessing request and optionally set
rucio_rule_id, workflow_name, fileset_id, or error_message.
"""

import time

from WMCore.Database.DBFormatter import DBFormatter

class UpdateReprocessingStatus(DBFormatter):

    def execute(self, requestId, status, conn = None, transaction = False,
                rucioRuleId = None, workflowName = None,
                filesetId = None, errorMessage = None):

        setClauses = ["STATUS = :STATUS", "UPDATED_AT = :NOW"]
        binds = { 'ID' : requestId,
                  'STATUS' : status,
                  'NOW' : int(time.time()) }

        if rucioRuleId is not None:
            setClauses.append("RUCIO_RULE_ID = :RUCIO_RULE_ID")
            binds['RUCIO_RULE_ID'] = rucioRuleId

        if workflowName is not None:
            setClauses.append("WORKFLOW_NAME = :WORKFLOW_NAME")
            binds['WORKFLOW_NAME'] = workflowName

        if filesetId is not None:
            setClauses.append("FILESET_ID = :FILESET_ID")
            binds['FILESET_ID'] = filesetId

        if errorMessage is not None:
            setClauses.append("ERROR_MESSAGE = :ERROR_MESSAGE")
            binds['ERROR_MESSAGE'] = errorMessage[:2000]

        sql = """UPDATE reprocessing_request
                 SET %s
                 WHERE id = :ID
                 """ % ", ".join(setClauses)

        self.dbi.processData(sql, binds, conn = conn,
                             transaction = transaction)

        return
