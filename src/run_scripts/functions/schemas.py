# -*- coding: utf-8 -*-
"""
Created on Fri Jun 01 15:15:41 2018

@author: agatab
"""
blank_dates = ['2017-11-21', '2017-12-08', '2018-01-26', '2018-02-07',
       '2018-02-13', '2018-02-14', '2018-03-07', '2018-03-13',
       '2018-03-22', '2018-04-26', '2018-05-11', '2018-05-24',
       '2018-06-14', '2018-07-20']

internal_dates = ['2017-12-04', '2018-01-12', '2018-02-12', '2018-02-13',
                  '2018-02-14', '2018-03-13', '2018-03-14', '2018-03-28',
                  '2018-03-29', '2018-04-08', '2018-04-09', '2018-04-30',
                  '2018-05-01', '2018-05-21', '2018-06-27', '2018-07-09']

valid_rois = ['VISp1','VISp2/3', 'VISp4', 'VISp5', 'VISp6a', 'VISp6b', 
              'FCx1', 'FCx2', 'FCx3', 'FCx3a', 'FCx3b', 'FCx3c', 'FCx4', 
              'FCx5', 'FCx6', 'TCx1', 'TCx2', 'TCx3', 'TCx3a', 'TCx3b', 
              'TCx3c', 'TCx4', 'TCx5', 'TCx6', 'TEa1', 'TEa2/3', 'TEa4', 
              'TEa5', 'TEa6a', 'TEa6b', 'dLGNMagnocellular', 
              'dLGNParvocellular', 'dLGNCore', 'dLGNShell']

schemas = {
    "met_slice": {
        'limsSpecName': {
            'type': 'string',
            'required': True,
            'minlength': 10
        },
        'date': {
            'type': 'string', 
            'required': True, 
            'regex': {
                'pattern': '\d{4}-[0-3]\d-[0-3]\d [0-2]\d:[0-5]\d:[0-5]\d (-|\+)?[0-1]\d:00',
                'fail_message': 'date is not in YYYY-MM-DD HH:MM:SS UTC format'
            }
        },
        'formVersion': {
            'type': 'string', 
            'required': True,
            'regex': {
                'pattern': '\d.\d.\d',
                'fail_message': 'invalid JEM version'
            }
        },
        'rigOperator': {
            'type': 'string',
            'required': True
        },
        'rigNumber': {
            'type': 'string',
            'required': True
        },
        'acsfProductionDate': {
            'type': 'string', 
            'required': True, 
            'regex': {
                'pattern': '20[1-2][0-9]-[0-3]\d-[0-3]\d',
                'fail_message': 'date is not in YYYY-MM-DD format'
            }
        },
        'blankFillDate': {
            'type': 'string', 
            'required': True,
            'allowed': blank_dates,
            'regex': {
                'pattern': '20[1-2][0-9]-[0-3]\d-[0-3]\d',
                'fail_message': 'date is not in YYYY-MM-DD format'
            }
        },
        'internalFillDate': {
            'type': 'string', 
            'required': True,
            'allowed': internal_dates,
            'regex': {
                'pattern': '20[1-2][0-9]-[0-3]\d-[0-3]\d',
                'fail_message': 'date is not in YYYY-MM-DD format'
            }
        },
        'flipped': {
            'type': 'string',
            'required': True,  
            'allowed': ["Yes", "No"]
        },
        'pipettes': {
            'type': 'list',
            'required': True
        }
    },
    'met_pipette' : {
        #'pipetteSpecName': {
        #    'type': 'string',
        #    'required': True,
        #    'regex': {
        #        'pattern': '',
        #        'fail_message': ''
        #    }
        #},
        'approach.cellHealth': {
                'type': 'string',
        'required': False,
        'allowed': ['1','2','3','4','5']
        },    
        'approach.creCell': {
            'type': 'string',
            'required': True,
            'allowed': ['Cre+','Cre-','None']
        },    
        'approach.pilotName':{
            'type': 'string',
            'required': True
        },
        'approach.sliceHealth': {
            'type': 'string',
            'required': False,
            'allowed': ['1','2','3','4','5']
        },
        'autoRoi': {
            'type': 'string',
            'required': True
        },
        'depth': {
            'type': 'float',
            'required': True
        },
        'manualRoi': {
            'type': 'string',
            'required': True,
            'allowed': valid_rois
        }, 
        'recording.pipetteR': {
            'type': 'float',
            'required': True
        },
        'recording.timeStart': {
            'type': 'string', 
            'required': True, 
            'regex': {
                'pattern': '[0-2]\d:[0-5]\d:[0-5]\d (-|\+)?[0-1]\d:00',
                'fail_message': 'time is not in HH:MM:SS UTC format'
            }
        },
        'recording.timeWholeCellStart': {
            'type': 'string', 
            'required': True, 
            'regex': {
                'pattern': '[0-2]\d:[0-5]\d:[0-5]\d (-|\+)?[0-1]\d:00',
                'fail_message': 'time is not in HH:MM:SS UTC format'
            }
        },
        'status': {
            'type': 'string',
            'required': True,
            'allowed': ['SUCCESS', 'FAILURE']
        }
    },
    'met_tube' : {
        'extraction.endPipetteR': {
            'type': 'float',
            'required': True
        },
        'extraction.extractionObservations': {
            #'type': 'string',
            'required': False,
        },
        'extraction.nucleus': {
            #'type': 'string',
            'required': False,
            'allowed': ['no', 'intentionally', 'not_intentionally']
        },    
        'extraction.postPatch':{
            'type': 'string',
            'required': True,
            'allowed': ['nucleus_present', 'nucleus_absent', 'entire_cell']
        },
        'extraction.pressureApplied': {
            'type': 'float',
            'required': True
        },
        'extraction.retractionPressureApplied': {
            'type': 'float',
            'required': True
        },
        'extraction.sampleObservations': {
            #'type': 'string',
            'required': False
        },
        'extraction.timeExtractionEnd': {
            'type': 'string', 
            'required': True, 
            'regex': {
                'pattern': '[0-2]\d:[0-5]\d:[0-5]\d (-|\+)?[0-1]\d:00',
                'fail_message': 'time is not in HH:MM:SS UTC format'
            }
        },
        'extraction.timeExtractionStart': {
            'type': 'string', 
            'required': True, 
            'regex': {
                'pattern': '[0-2]\d:[0-5]\d:[0-5]\d (-|\+)?[0-1]\d:00',
                'fail_message': 'time is not in HH:MM:SS UTC format'
            }
        },
        'extraction.timeRetractionEnd': {
            'type': 'string', 
            'required': True, 
            'regex': {
                'pattern': '[0-2]\d:[0-5]\d:[0-5]\d (-|\+)?[0-1]\d:00',
                'fail_message': 'time is not in HH:MM:SS UTC format'
            }
        },
        'extraction.tubeID': {
            'type': 'string', 
            'required': True, 
            'regex': {
                'pattern': '^(P[A-Z0-9]S4_([0-9]{2}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01]))_[0-9]{3}_A01)|(NA)$',
                #'pattern': '^(P[A-Z0-9]S4_(\d\d(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01]))_\d\d\d_A01)|(NA)$',
                'fail_message': 'tube id does not match P{}S4_YYMMDD_{}{}{}_A01 or NA format'
            }
        }
    },
    "met_slice_outdated": {
        'limsSpecName': {
            'type': 'string',
            'required': True,
            'minlength': 10
        },
        'date': {
            'type': 'string', 
            'required': True
        },
        'formVersion': {
            'type': 'string', 
            'required': True,
            'regex': {
                'pattern': '\d.\d.\d',
                'fail_message': 'invalid JEM version'
            }
        },
        'rigOperator': {
            'type': 'string',
            'required': True
        },
        'rigNumber': {
            'required': True
        },
        'acsfProductionDate': {
            'type': 'string', 
            'required': True
        },
        'flipped': {
            'type': 'string',
            'required': True,  
            'allowed': ["Yes", "No"]
        },
        'pipettesPatchSeqPilot': {
            'type': 'list',
            'required': True
        }
    }
}

