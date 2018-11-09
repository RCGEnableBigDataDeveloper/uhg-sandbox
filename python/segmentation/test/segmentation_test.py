import sys
from segmentation import Segmentation

if __name__ == '__main__':
    
    segmentation = Segmentation()
    
    print("running segmentation test")
    
    data = ('''{
        "type": "auth",
        "resources": [{
            "name": "BASE_TABLE",
            "location" : "/datalake/uhc/ei/pi_ara_mirroring/hive/warehouse/",
            "database" : "ara",
            "segments": [{
                "name": "TABLE_000",
                "segment": "000",
                "groups": ["g1", "g2", "g3"]
            },{
                "name": "TABLE_001",
                "segment": "001",
                "groups": ["g4", "g5", "g6"]
            },{
                "name": "TABLE_002",
                "segment": "002",
                "groups": ["g7", "g8", "g9"]
            }]
        }]
    }''')   
    
    segmentation.segment(data)     
    sys.exit(0)
