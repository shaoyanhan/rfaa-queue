
class TaskElement:
    def __init__(self, step, params):
        self._step = step  # 初始化任务步骤
        self._params = params  # 初始化受保护的任务参数
        self._priority = None  # 初始化优先级
        self._pid = None  # 初始化进程 ID
        self._cpu = None  # 预分配的 cpu 数量
        self._mem = None  # 预分配的内存数量

    @property
    def step(self):
        """任务步骤的 getter 方法"""
        return self._step
    
    @step.setter
    def step(self, value):
        """任务步骤的 setter 方法"""
        self._step = value

    @property
    def priority(self):
        """优先级的 getter 方法"""
        return self._priority

    @priority.setter
    def priority(self, value):
        """优先级的 setter 方法，确保只有内部逻辑可设置优先级"""
        if not isinstance(value, (int, float, type(None))):  # 检查是否为数字或 None
            raise ValueError("Priority must be a numeric value or None.")
        self._priority = value

    @property
    def params(self):
        """任务参数的 getter 方法"""
        return self._params

    @params.setter
    def params(self, new_params):
        """任务参数的 setter 方法，更新任务参数并重置优先级"""
        self._params = new_params
        self._priority = None  # 重置优先级为 None，表示需要重新计算

    @property
    def pid(self):
        """进程 ID 的 getter 方法"""
        return self._pid
    
    @pid.setter
    def pid(self, value):
        """进程 ID 的 setter 方法"""
        self._pid = value

    @property
    def cpu(self):
        """cpu 数量的 getter 方法"""
        return self._cpu
    
    @cpu.setter
    def cpu(self, value):
        """cpu 数量的 setter 方法"""
        self._cpu = value

    @property
    def mem(self):
        """内存数量的 getter 方法"""
        return self._mem
    
    @mem.setter
    def mem(self, value):
        """内存数量的 setter 方法"""
        self._mem = value

    # 重写 __repr__ 方法，用于打印任务信息 
    def __repr__(self):
        return f"TaskElement(step={self.step}, params={self.params}, priority={self.priority}), PID={self.pid}, CPU={self.cpu}, MEM={self.mem}"

    # 重写 __lt__ 方法，用于加入优先级队列时比较任务优先级
    def __lt__(self, other):
        if self.priority is None or other.priority is None:
            raise ValueError("Cannot compare tasks without a priority set.")
        return self.priority < other.priority  # 比较任务优先级以便队列排序