class Config:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance.args = {}  # 用于存储全局参数
        return cls._instance
    
    def __init__(self):
        if Config._instance is not None:
            raise Exception("This class is a singleton! Use the 'global_config' instance.")
    
    def set_args(self, args):
        self.args = args
    
    def get_args(self):
        return self.args
    
global_config = Config()