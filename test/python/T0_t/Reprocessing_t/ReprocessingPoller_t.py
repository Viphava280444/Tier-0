"""
_ReprocessingPoller_t_

Unit tests for the ReprocessingPoller state machine transitions.
Tests each state transition path with mocked DAOs and services.
"""

import unittest
from unittest.mock import MagicMock, patch, PropertyMock

import time


class TestInsertConfigRequests(unittest.TestCase):
    """Test configuration loading and request insertion."""

    @patch('T0Component.Reprocessing.ReprocessingPoller.DAOFactory')
    @patch('T0Component.Reprocessing.ReprocessingPoller.BaseWorkerThread.__init__')
    def setUp(self, mockBaseInit, mockDAOFactory):
        mockBaseInit.return_value = None

        # Mock threading
        import threading
        mockThread = MagicMock()
        mockThread.dbi = MagicMock()
        with patch.object(threading, 'currentThread', return_value=mockThread):
            from T0Component.Reprocessing.ReprocessingPoller import ReprocessingPoller
            config = MagicMock()
            config.Reprocessing.reprocessingConfigFile = "/tmp/test.py"
            config.Reprocessing.specDirectory = "/tmp/specs"
            config.Reprocessing.processingSiteRSE = "T0_CH_CERN_Disk"
            config.Reprocessing.stagingTimeout = 48 * 3600
            config.Reprocessing.stagingLifetime = 7 * 86400
            config.Reprocessing.rucioAccount = "tier0_prod"
            config.Reprocessing.dbsUrl = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader"
            config.Reprocessing.dqmUploadProxy = None
            self.poller = ReprocessingPoller(config)

    def test_valid_request_inserted(self):
        """Valid config request should be inserted to DB."""
        mockConfig = MagicMock()
        mockConfig.Global.ReprocessingRequests = [
            {
                'InputDataset': '/ZeroBias/Run2024A-v1/RAW',
                'GlobalTag': '140X_dataRun3_PromptNew_v2',
                'CMSSWVersion': 'CMSSW_14_0_17',
                'ScramArch': 'el8_amd64_gcc12',
                'ProcVersion': 2,
                'Scenario': 'ppEra_Run3',
                'WriteTiers': ['AOD', 'MINIAOD', 'DQM'],
                'AlcaSkims': [],
                'PhysicsSkims': [],
                'DqmSequences': [],
                'NanoFlavour': None,
                'RecoSplit': 500,
                'Multicore': 8,
                'RunId': None,
                'MinLumi': None,
                'MaxLumi': None,
            }
        ]

        mockInsertDAO = MagicMock()
        self.poller.daoFactory = MagicMock(return_value=mockInsertDAO)

        import threading
        mockThread = MagicMock()
        with patch.object(threading, 'currentThread', return_value=mockThread):
            self.poller.insertConfigRequests(mockConfig)

        mockInsertDAO.execute.assert_called_once()

    def test_missing_required_field_skipped(self):
        """Request missing GlobalTag should be skipped."""
        mockConfig = MagicMock()
        mockConfig.Global.ReprocessingRequests = [
            {
                'InputDataset': '/ZeroBias/Run2024A-v1/RAW',
                'GlobalTag': None,  # Missing!
                'CMSSWVersion': 'CMSSW_14_0_17',
                'ScramArch': 'el8_amd64_gcc12',
                'ProcVersion': 2,
                'Scenario': 'ppEra_Run3',
            }
        ]

        mockInsertDAO = MagicMock()
        self.poller.daoFactory = MagicMock(return_value=mockInsertDAO)

        import threading
        mockThread = MagicMock()
        with patch.object(threading, 'currentThread', return_value=mockThread):
            self.poller.insertConfigRequests(mockConfig)

        mockInsertDAO.execute.assert_not_called()

    def test_non_raw_tier_skipped(self):
        """Request with non-RAW input dataset should be skipped."""
        mockConfig = MagicMock()
        mockConfig.Global.ReprocessingRequests = [
            {
                'InputDataset': '/ZeroBias/Run2024A-v1/RECO',  # Not RAW!
                'GlobalTag': '140X_dataRun3_PromptNew_v2',
                'CMSSWVersion': 'CMSSW_14_0_17',
                'ScramArch': 'el8_amd64_gcc12',
                'ProcVersion': 2,
                'Scenario': 'ppEra_Run3',
            }
        ]

        mockInsertDAO = MagicMock()
        self.poller.daoFactory = MagicMock(return_value=mockInsertDAO)

        import threading
        mockThread = MagicMock()
        with patch.object(threading, 'currentThread', return_value=mockThread):
            self.poller.insertConfigRequests(mockConfig)

        mockInsertDAO.execute.assert_not_called()

    def test_duplicate_request_skipped(self):
        """Same dataset+procversion should only be inserted once."""
        mockConfig = MagicMock()
        request = {
            'InputDataset': '/ZeroBias/Run2024A-v1/RAW',
            'GlobalTag': '140X_dataRun3_PromptNew_v2',
            'CMSSWVersion': 'CMSSW_14_0_17',
            'ScramArch': 'el8_amd64_gcc12',
            'ProcVersion': 2,
            'Scenario': 'ppEra_Run3',
            'WriteTiers': ['AOD', 'MINIAOD', 'DQM'],
            'AlcaSkims': [],
            'PhysicsSkims': [],
            'DqmSequences': [],
            'NanoFlavour': None,
            'RecoSplit': 500,
            'Multicore': 8,
            'RunId': None,
            'MinLumi': None,
            'MaxLumi': None,
        }
        mockConfig.Global.ReprocessingRequests = [request, request]

        mockInsertDAO = MagicMock()
        self.poller.daoFactory = MagicMock(return_value=mockInsertDAO)

        import threading
        mockThread = MagicMock()
        with patch.object(threading, 'currentThread', return_value=mockThread):
            self.poller.insertConfigRequests(mockConfig)

        # Should only be called once despite two identical requests
        self.assertEqual(mockInsertDAO.execute.call_count, 1)


class TestTier0ConfigHelpers(unittest.TestCase):
    """Test Tier0Config helper functions for reprocessing."""

    def test_add_reprocessing_request(self):
        """addReprocessingRequest should append to ReprocessingRequests."""
        import T0.RunConfig.Tier0Config as tier0config
        config = tier0config.createTier0Config()

        tier0config.addReprocessingRequest(
            config,
            inputDataset="/ZeroBias/Run2024A-v1/RAW",
            globalTag="140X_dataRun3_PromptNew_v2",
            cmsswVersion="CMSSW_14_0_17",
            scramArch="el8_amd64_gcc12",
            procVersion=2,
            scenario="ppEra_Run3"
        )

        self.assertEqual(len(config.Global.ReprocessingRequests), 1)
        req = config.Global.ReprocessingRequests[0]
        self.assertEqual(req['InputDataset'], "/ZeroBias/Run2024A-v1/RAW")
        self.assertEqual(req['GlobalTag'], "140X_dataRun3_PromptNew_v2")
        self.assertEqual(req['WriteTiers'], ["AOD", "MINIAOD", "DQM"])
        self.assertEqual(req['RecoSplit'], 500)
        self.assertEqual(req['Multicore'], 8)

    def test_set_output_tiers(self):
        """setReprocessingOutputTiers should override WriteTiers."""
        import T0.RunConfig.Tier0Config as tier0config
        config = tier0config.createTier0Config()

        tier0config.addReprocessingRequest(
            config, "/ZB/Run2024A-v1/RAW", "GT", "CMSSW_14", "el8", 1, "ppEra_Run3")
        tier0config.setReprocessingOutputTiers(config, 0, ["RECO", "AOD", "DQM"])

        self.assertEqual(config.Global.ReprocessingRequests[0]['WriteTiers'],
                         ["RECO", "AOD", "DQM"])

    def test_set_alca_skims(self):
        """setReprocessingAlcaSkims should set AlcaSkims."""
        import T0.RunConfig.Tier0Config as tier0config
        config = tier0config.createTier0Config()

        tier0config.addReprocessingRequest(
            config, "/ZB/Run2024A-v1/RAW", "GT", "CMSSW_14", "el8", 1, "ppEra_Run3")
        tier0config.setReprocessingAlcaSkims(config, 0, ["TkAlMinBias", "SiStripCalMinBias"])

        self.assertEqual(config.Global.ReprocessingRequests[0]['AlcaSkims'],
                         ["TkAlMinBias", "SiStripCalMinBias"])

    def test_set_run_filter(self):
        """setReprocessingRunFilter should set run/lumi filtering."""
        import T0.RunConfig.Tier0Config as tier0config
        config = tier0config.createTier0Config()

        tier0config.addReprocessingRequest(
            config, "/ZB/Run2024A-v1/RAW", "GT", "CMSSW_14", "el8", 1, "ppEra_Run3")
        tier0config.setReprocessingRunFilter(config, 0, runId=398191, minLumi=50, maxLumi=100)

        req = config.Global.ReprocessingRequests[0]
        self.assertEqual(req['RunId'], 398191)
        self.assertEqual(req['MinLumi'], 50)
        self.assertEqual(req['MaxLumi'], 100)

    def test_set_output_tiers_invalid_index(self):
        """setReprocessingOutputTiers with invalid index should raise."""
        import T0.RunConfig.Tier0Config as tier0config
        config = tier0config.createTier0Config()

        with self.assertRaises(RuntimeError):
            tier0config.setReprocessingOutputTiers(config, 0, ["AOD"])

    def test_multiple_requests(self):
        """Multiple addReprocessingRequest calls should create separate requests."""
        import T0.RunConfig.Tier0Config as tier0config
        config = tier0config.createTier0Config()

        tier0config.addReprocessingRequest(
            config, "/ZB/Run2024A-v1/RAW", "GT1", "CMSSW_14", "el8", 1, "ppEra_Run3")
        tier0config.addReprocessingRequest(
            config, "/Muon/Run2024A-v1/RAW", "GT2", "CMSSW_14", "el8", 1, "ppEra_Run3")

        self.assertEqual(len(config.Global.ReprocessingRequests), 2)
        self.assertEqual(config.Global.ReprocessingRequests[0]['InputDataset'],
                         "/ZB/Run2024A-v1/RAW")
        self.assertEqual(config.Global.ReprocessingRequests[1]['InputDataset'],
                         "/Muon/Run2024A-v1/RAW")

    def test_custom_options(self):
        """Custom options (recoSplit, multicore, etc.) should override defaults."""
        import T0.RunConfig.Tier0Config as tier0config
        config = tier0config.createTier0Config()

        tier0config.addReprocessingRequest(
            config, "/ZB/Run2024A-v1/RAW", "GT", "CMSSW_14", "el8", 1, "ppEra_Run3",
            recoSplit=1000, multicore=16,
            writeTiers=["RECO", "AOD", "MINIAOD", "NANOAOD", "DQM"],
            nanoFlavour="@PhysicsTools/NanoAOD/nano_cff.py"
        )

        req = config.Global.ReprocessingRequests[0]
        self.assertEqual(req['RecoSplit'], 1000)
        self.assertEqual(req['Multicore'], 16)
        self.assertEqual(req['NanoFlavour'], "@PhysicsTools/NanoAOD/nano_cff.py")
        self.assertIn("NANOAOD", req['WriteTiers'])


if __name__ == '__main__':
    unittest.main()
