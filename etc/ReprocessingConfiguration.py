"""
_ReprocessingConfiguration_

Configuration for reprocessing from existing RAW datasets.

Operators: edit this file to declare reprocessing requests,
then start/restart the Reprocessing component.

Usage pattern (same as ReplayOfflineConfiguration.py):
  1. Call addReprocessingRequest() for each dataset to reprocess
  2. Optionally call setReprocessingOutputTiers/AlcaSkims/RunFilter
  3. The ReprocessingPoller reads this config and handles the rest
"""
from __future__ import print_function

from T0.RunConfig.Tier0Config import createTier0Config
from T0.RunConfig.Tier0Config import addReprocessingRequest
from T0.RunConfig.Tier0Config import setReprocessingOutputTiers
from T0.RunConfig.Tier0Config import setReprocessingAlcaSkims
from T0.RunConfig.Tier0Config import setReprocessingRunFilter

# Create the configuration object
tier0Config = createTier0Config()

#
# Example reprocessing request:
#
# addReprocessingRequest(tier0Config,
#     inputDataset = "/ZeroBias/Run2024A-v1/RAW",
#     globalTag = "140X_dataRun3_PromptNew_v2",
#     cmsswVersion = "CMSSW_14_0_17",
#     scramArch = "el8_amd64_gcc12",
#     procVersion = 2,
#     scenario = "ppEra_Run3",
# )
#
# Optional: override output tiers (default: ["AOD", "MINIAOD", "DQM"])
# setReprocessingOutputTiers(tier0Config, 0, ["RECO", "AOD", "MINIAOD", "DQM"])
#
# Optional: add AlcaSkims
# setReprocessingAlcaSkims(tier0Config, 0, ["TkAlMinBias", "SiStripCalMinBias"])
#
# Optional: filter to specific run and lumi range
# setReprocessingRunFilter(tier0Config, 0, runId=398191, minLumi=50, maxLumi=100)
