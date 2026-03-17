"""
_DataStagingAPI_t_

Unit tests for the DataStagingAPI module.
Tests DBS discovery and Rucio staging logic with mocked services.
"""

import unittest
from unittest.mock import MagicMock, patch

from T0.Reprocessing.DataStagingAPI import (
    discoverRAWFiles,
    classifyFileLocations,
    createStagingRules,
    checkStagingStatus,
    deleteStagingRule,
)


class TestDiscoverRAWFiles(unittest.TestCase):

    def setUp(self):
        self.dbsReader = MagicMock()

    def test_dataset_not_found(self):
        """Dataset not in DBS should raise RuntimeError."""
        self.dbsReader.dbs.listDatasets.return_value = []
        with self.assertRaises(RuntimeError):
            discoverRAWFiles(self.dbsReader, "/NotExist/Run2024A-v1/RAW")

    def test_no_files_found(self):
        """Dataset exists but has no files should raise RuntimeError."""
        self.dbsReader.dbs.listDatasets.return_value = [{"dataset": "/ZB/Run2024A-v1/RAW"}]
        self.dbsReader.dbs.listFileArray.return_value = []
        with self.assertRaises(RuntimeError):
            discoverRAWFiles(self.dbsReader, "/ZB/Run2024A-v1/RAW")

    def test_basic_discovery(self):
        """Basic file discovery returns files organized by block."""
        self.dbsReader.dbs.listDatasets.return_value = [{"dataset": "/ZB/Run2024A-v1/RAW"}]
        self.dbsReader.dbs.listFileArray.return_value = [
            {'logical_file_name': '/store/data/file1.root',
             'block_name': '/ZB/Run2024A-v1/RAW#block1',
             'file_size': 1000, 'event_count': 100},
            {'logical_file_name': '/store/data/file2.root',
             'block_name': '/ZB/Run2024A-v1/RAW#block1',
             'file_size': 2000, 'event_count': 200},
            {'logical_file_name': '/store/data/file3.root',
             'block_name': '/ZB/Run2024A-v1/RAW#block2',
             'file_size': 3000, 'event_count': 300},
        ]

        result = discoverRAWFiles(self.dbsReader, "/ZB/Run2024A-v1/RAW")

        self.assertEqual(len(result), 2)  # 2 blocks
        self.assertEqual(len(result['/ZB/Run2024A-v1/RAW#block1']), 2)
        self.assertEqual(len(result['/ZB/Run2024A-v1/RAW#block2']), 1)

    def test_run_filter(self):
        """Run filtering should pass run_num to DBS query."""
        self.dbsReader.dbs.listDatasets.return_value = [{"dataset": "/ZB/Run2024A-v1/RAW"}]
        self.dbsReader.dbs.listFileArray.return_value = [
            {'logical_file_name': '/store/data/file1.root',
             'block_name': '/ZB/Run2024A-v1/RAW#block1',
             'file_size': 1000, 'event_count': 100},
        ]

        discoverRAWFiles(self.dbsReader, "/ZB/Run2024A-v1/RAW", runId=398191)

        self.dbsReader.dbs.listFileArray.assert_called_with(
            dataset="/ZB/Run2024A-v1/RAW", run_num=398191, detail=True
        )


class TestClassifyFileLocations(unittest.TestCase):

    def setUp(self):
        self.rucioClient = MagicMock()

    def test_all_on_disk(self):
        """All files on disk at processing site."""
        filesByBlock = {
            'block1': [{'lfn': '/store/file1.root', 'size': 1000, 'events': 100}],
        }

        self.rucioClient.list_replicas.return_value = iter([
            {'rses': {'T0_CH_CERN_Disk': ['root://path']}}
        ])

        result = classifyFileLocations(self.rucioClient, filesByBlock, 'T0_CH_CERN_Disk')

        self.assertEqual(result['on_disk'], ['block1'])
        self.assertEqual(result['on_tape'], [])
        self.assertEqual(result['missing'], [])

    def test_on_tape(self):
        """Files only on tape RSE."""
        filesByBlock = {
            'block1': [{'lfn': '/store/file1.root', 'size': 1000, 'events': 100}],
        }

        self.rucioClient.list_replicas.return_value = iter([
            {'rses': {'T1_US_FNAL_Tape': ['srm://path']}}
        ])

        result = classifyFileLocations(self.rucioClient, filesByBlock, 'T0_CH_CERN_Disk')

        self.assertEqual(result['on_disk'], [])
        self.assertEqual(result['on_tape'], ['block1'])
        self.assertEqual(result['missing'], [])

    def test_missing_files(self):
        """Files with no replicas are classified as missing."""
        filesByBlock = {
            'block1': [{'lfn': '/store/file1.root', 'size': 1000, 'events': 100}],
        }

        self.rucioClient.list_replicas.return_value = iter([
            {'rses': {}}
        ])

        result = classifyFileLocations(self.rucioClient, filesByBlock, 'T0_CH_CERN_Disk')

        self.assertIn('/store/file1.root', result['missing'])

    def test_mixed_disk_and_tape(self):
        """Some blocks on disk, some need staging."""
        filesByBlock = {
            'block1': [{'lfn': '/store/file1.root', 'size': 1000, 'events': 100}],
            'block2': [{'lfn': '/store/file2.root', 'size': 2000, 'events': 200}],
        }

        def mock_list_replicas(dids, schemes=None):
            lfn = dids[0]['name']
            if lfn == '/store/file1.root':
                return iter([{'rses': {'T0_CH_CERN_Disk': ['root://path']}}])
            else:
                return iter([{'rses': {'T1_US_FNAL_Tape': ['srm://path']}}])

        self.rucioClient.list_replicas.side_effect = mock_list_replicas

        result = classifyFileLocations(self.rucioClient, filesByBlock, 'T0_CH_CERN_Disk')

        self.assertEqual(result['on_disk'], ['block1'])
        self.assertEqual(result['on_tape'], ['block2'])


class TestCreateStagingRules(unittest.TestCase):

    def test_creates_rules_per_block(self):
        """Should create one Rucio rule per block."""
        rucioClient = MagicMock()
        rucioClient.add_replication_rule.return_value = ['rule-id-1']

        blocks = ['block1', 'block2']
        ruleIds = createStagingRules(rucioClient, blocks, 'T0_CH_CERN_Disk')

        self.assertEqual(rucioClient.add_replication_rule.call_count, 2)
        self.assertEqual(len(ruleIds), 2)

    def test_rule_parameters(self):
        """Rule should use correct parameters."""
        rucioClient = MagicMock()
        rucioClient.add_replication_rule.return_value = ['rule-id-1']

        createStagingRules(rucioClient, ['block1'], 'T0_CH_CERN_Disk',
                           lifetime=604800, account='tier0_prod')

        rucioClient.add_replication_rule.assert_called_once()
        call_kwargs = rucioClient.add_replication_rule.call_args
        self.assertEqual(call_kwargs[1]['lifetime'], 604800)
        self.assertEqual(call_kwargs[1]['activity'], 'T0Reprocessing')
        self.assertEqual(call_kwargs[1]['priority'], 5)


class TestCheckStagingStatus(unittest.TestCase):

    def test_all_ok(self):
        """All rules OK should return OK status."""
        rucioClient = MagicMock()
        rucioClient.get_replication_rule.return_value = {
            'state': 'OK', 'locks_ok_cnt': 10,
            'locks_replicating_cnt': 0, 'locks_stuck_cnt': 0
        }

        result = checkStagingStatus(rucioClient, ['rule1', 'rule2'])

        self.assertEqual(result['status'], 'OK')
        self.assertEqual(result['ok_count'], 2)

    def test_replicating(self):
        """Rules still transferring should return REPLICATING."""
        rucioClient = MagicMock()
        rucioClient.get_replication_rule.return_value = {
            'state': 'REPLICATING', 'locks_ok_cnt': 5,
            'locks_replicating_cnt': 5, 'locks_stuck_cnt': 0
        }

        result = checkStagingStatus(rucioClient, ['rule1'])

        self.assertEqual(result['status'], 'REPLICATING')

    def test_stuck(self):
        """Stuck rules should return STUCK."""
        rucioClient = MagicMock()
        rucioClient.get_replication_rule.return_value = {
            'state': 'STUCK', 'locks_ok_cnt': 3,
            'locks_replicating_cnt': 0, 'locks_stuck_cnt': 7
        }

        result = checkStagingStatus(rucioClient, ['rule1'])

        self.assertEqual(result['status'], 'STUCK')


class TestDeleteStagingRule(unittest.TestCase):

    def test_delete_rules(self):
        """Should call delete for each rule ID."""
        rucioClient = MagicMock()
        deleteStagingRule(rucioClient, ['rule1', 'rule2'])
        self.assertEqual(rucioClient.delete_replication_rule.call_count, 2)


if __name__ == '__main__':
    unittest.main()
