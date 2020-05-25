class filesystem_data():
    def __init__(self):
        self.initfiles = []
        self.tempfile=''
        self.updatedfiles=[]
        self.updatedfile=''
        self.directorywatch=True
        self.beamprocessingstack=[]
        self.recordqueue=0
        self.filewatch=False