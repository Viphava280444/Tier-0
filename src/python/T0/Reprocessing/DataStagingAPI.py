"""
_DataStagingAPI_

API for discovering RAW file locations and staging data from tape.

Uses DBS3Reader for file discovery and Rucio client for
replica location queries and replication rule management.
"""

import logging


def discoverRAWFiles(dbsReader, inputDataset, runId=None, minLumi=None, maxLumi=None):
    """
    _discoverRAWFiles_

    Query DBS3 for the file list of the input RAW dataset,
    optionally filtered by run and lumi range.

    Returns a dict keyed by block name, each containing a list of file dicts
    with keys: lfn, size, events, run_lumi_list
    """
    # Verify dataset exists
    datasets = dbsReader.dbs.listDatasets(dataset=inputDataset)
    if not datasets:
        raise RuntimeError("Dataset %s not found in DBS" % inputDataset)

    # Get files with lumi information
    if runId is not None:
        fileDetails = dbsReader.dbs.listFileArray(
            dataset=inputDataset,
            run_num=runId,
            detail=True
        )
    else:
        fileDetails = dbsReader.dbs.listFileArray(
            dataset=inputDataset,
            detail=True
        )

    if not fileDetails:
        raise RuntimeError("No files found for dataset %s" % inputDataset)

    # If lumi filtering is needed, get lumi info and filter
    if minLumi is not None or maxLumi is not None:
        filteredFiles = []
        for fileInfo in fileDetails:
            lfn = fileInfo['logical_file_name']
            fileLumis = dbsReader.dbs.listFileLumis(logical_file_name=lfn)
            keep = False
            for lumiInfo in fileLumis:
                for lumi in lumiInfo.get('lumi_section_num', []):
                    if (minLumi is None or lumi >= minLumi) and \
                       (maxLumi is None or lumi <= maxLumi):
                        keep = True
                        break
                if keep:
                    break
            if keep:
                filteredFiles.append(fileInfo)
        fileDetails = filteredFiles

    if not fileDetails:
        raise RuntimeError("No files found after lumi filtering for dataset %s" % inputDataset)

    # Organize by block
    filesByBlock = {}
    for fileInfo in fileDetails:
        blockName = fileInfo.get('block_name', 'unknown')
        if blockName not in filesByBlock:
            filesByBlock[blockName] = []
        filesByBlock[blockName].append({
            'lfn': fileInfo['logical_file_name'],
            'size': fileInfo.get('file_size', 0),
            'events': fileInfo.get('event_count', 0),
        })

    logging.info("Discovered %d files in %d blocks for %s",
                 sum(len(v) for v in filesByBlock.values()),
                 len(filesByBlock), inputDataset)

    return filesByBlock


def classifyFileLocations(rucioClient, filesByBlock, processingSiteRSE):
    """
    _classifyFileLocations_

    For each file, query Rucio for replica locations.
    Classify as on-disk, on-tape, or missing.

    Returns a dict with keys:
      'on_disk': list of block names with all files on disk
      'on_tape': list of block names needing staging
      'missing': list of LFNs with no replicas
    """
    onDiskBlocks = []
    onTapeBlocks = []
    missingFiles = []

    for blockName, files in filesByBlock.items():
        blockOnDisk = True
        blockHasTape = False

        for fileInfo in files:
            lfn = fileInfo['lfn']
            try:
                replicas = list(rucioClient.list_replicas(
                    [{'scope': 'cms', 'name': lfn}],
                    schemes=['root', 'srm']
                ))
            except Exception:
                logging.warning("Failed to query replicas for %s", lfn)
                missingFiles.append(lfn)
                blockOnDisk = False
                continue

            if not replicas or not replicas[0].get('rses'):
                missingFiles.append(lfn)
                blockOnDisk = False
                continue

            rses = replicas[0]['rses']
            if processingSiteRSE not in rses:
                blockOnDisk = False
                # Check if file exists anywhere (tape or other disk)
                if rses:
                    blockHasTape = True
                else:
                    missingFiles.append(lfn)

        if blockOnDisk:
            onDiskBlocks.append(blockName)
        elif blockHasTape or not missingFiles:
            onTapeBlocks.append(blockName)

    return {
        'on_disk': onDiskBlocks,
        'on_tape': onTapeBlocks,
        'missing': missingFiles,
    }


def createStagingRules(rucioClient, blocksToStage, processingSiteRSE,
                       lifetime=7*86400, account='tier0_prod'):
    """
    _createStagingRules_

    Create Rucio replication rules at block level to recall
    tape-resident data to the processing site.

    Returns a list of rule IDs.
    """
    ruleIds = []

    for blockName in blocksToStage:
        try:
            dids = [{'scope': 'cms', 'name': blockName}]
            rules = rucioClient.add_replication_rule(
                dids,
                copies=1,
                rse_expression=processingSiteRSE,
                lifetime=lifetime,
                grouping='ALL',
                account=account,
                activity='T0Reprocessing',
                asynchronous=True,
                priority=5,
                comment='Tier0 reprocessing from RAW staging'
            )
            ruleIds.extend(rules)
            logging.info("Created staging rule %s for block %s -> %s",
                         rules, blockName, processingSiteRSE)
        except Exception as ex:
            logging.error("Failed to create staging rule for block %s: %s",
                          blockName, str(ex))
            raise

    return ruleIds


def checkStagingStatus(rucioClient, ruleIds):
    """
    _checkStagingStatus_

    Query Rucio rule status for staging rules.

    Returns a dict with:
      'status': 'OK', 'REPLICATING', 'STUCK', or 'FAILED'
      'ok_count': number of completed rules
      'total_count': total number of rules
      'locks_ok': total OK locks across all rules
      'locks_total': total locks across all rules
    """
    okCount = 0
    stuckCount = 0
    replicatingCount = 0
    failedCount = 0
    locksOk = 0
    locksTotal = 0

    for ruleId in ruleIds:
        try:
            ruleInfo = rucioClient.get_replication_rule(ruleId)
            state = ruleInfo['state']
            locksOk += ruleInfo.get('locks_ok_cnt', 0)
            locksTotal += (ruleInfo.get('locks_ok_cnt', 0) +
                          ruleInfo.get('locks_replicating_cnt', 0) +
                          ruleInfo.get('locks_stuck_cnt', 0))

            if state == 'OK':
                okCount += 1
            elif state == 'STUCK':
                stuckCount += 1
            elif state in ('SUSPENDED', 'WAITING_APPROVAL'):
                failedCount += 1
            else:
                replicatingCount += 1
        except Exception as ex:
            logging.error("Failed to check rule %s: %s", ruleId, str(ex))
            failedCount += 1

    totalCount = len(ruleIds)

    if failedCount > 0:
        overallStatus = 'FAILED'
    elif okCount == totalCount:
        overallStatus = 'OK'
    elif stuckCount > 0:
        overallStatus = 'STUCK'
    else:
        overallStatus = 'REPLICATING'

    return {
        'status': overallStatus,
        'ok_count': okCount,
        'total_count': totalCount,
        'locks_ok': locksOk,
        'locks_total': locksTotal,
    }


def deleteStagingRule(rucioClient, ruleIds):
    """
    _deleteStagingRule_

    Delete Rucio staging rules, releasing disk space reservations.
    Used when cancelling a reprocessing request.
    """
    for ruleId in ruleIds:
        try:
            rucioClient.delete_replication_rule(ruleId, purge_replicas=True)
            logging.info("Deleted staging rule %s", ruleId)
        except Exception as ex:
            logging.warning("Failed to delete staging rule %s: %s",
                           ruleId, str(ex))
