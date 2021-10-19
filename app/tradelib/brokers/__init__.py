'''
Broker Imports
'''

# Brokers
from .oanda import Oanda
from .fxcm import FXCM
from .ig import IG
from .spotware import Spotware
from .ib import IB
from .dukascopy import Dukascopy
from .fxopen import FXOpen

# Test Brokers
from .loadtest import LoadTest
from .test_brokers.test_spotware import TestSpotware