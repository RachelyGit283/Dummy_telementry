# """
# gpu_accelerator.py
# מחלקה ליצירת נתונים מואצים GPU עם fallback ל-numpy ו-python
# """

# import random
# from typing import List

# # בדיקת זמינות ספריות
# try:
#     import cupy as cp
#     HAS_CUPY = True
# except ImportError:
#     HAS_CUPY = False

# try:
#     import numpy as np
#     HAS_NUMPY = True
# except ImportError:
#     HAS_NUMPY = False

# class GPUAcceleratedGenerator:
#     """מחלקה ליצור נתונים מואץ GPU"""
    
#     def __init__(self, use_gpu: bool = True):
#         self.use_gpu = False
#         self.xp = None
        
#         if use_gpu:
#             try:
#                 import cupy as cp
#                 cp.cuda.Device(0).use()
#                 test_array = cp.array([1, 2, 3])
#                 test_result = cp.sum(test_array).get()
                
#                 self.use_gpu = True
#                 self.xp = cp
                
#             except (ImportError, Exception):
#                 self.use_gpu = False
#                 try:
#                     import numpy as np
#                     self.xp = np
#                 except ImportError:
#                     self.xp = None
    
#     def generate_batch_int(self, batch_size: int, num_fields: int, 
#                           max_bits: int = 32) -> List[List[int]]:
#         """יוצר batch של ערכי int"""
#         if not self.xp:
#             # Fallback לPython רגיל
#             return [[random.randint(0, (1 << max_bits) - 1) 
#                     for _ in range(num_fields)] 
#                    for _ in range(batch_size)]
        
#         max_val = (1 << max_bits) - 1
        
#         try:
#             batch = self.xp.random.randint(0, max_val, (batch_size, num_fields))
            
#             if self.use_gpu:
#                 return batch.get().tolist()
#             return batch.tolist()
            
#         except Exception:
#             # fallback
#             self.use_gpu = False
#             self.xp = None
#             return [[random.randint(0, max_val) 
#                     for _ in range(num_fields)] 
#                    for _ in range(batch_size)]

#     def generate_batch_float(self, batch_size: int, num_fields: int) -> List[List[float]]:
#         """יוצר batch של ערכי float"""
#         if not self.xp:
#             return [[random.random() for _ in range(num_fields)] 
#                    for _ in range(batch_size)]
        
#         try:
#             batch = self.xp.random.random((batch_size, num_fields))
            
#             if self.use_gpu:
#                 return batch.get().tolist()
#             return batch.tolist()
            
#         except Exception:
#             # fallback
#             self.use_gpu = False
#             self.xp = None
#             return [[random.random() for _ in range(num_fields)] 
#                    for _ in range(batch_size)]
"""
gpu_accelerator.py
GPU-accelerated data generation class with fallback to numpy and pure Python
"""

import random
from typing import List

# Check library availability
try:
    import cupy as cp
    HAS_CUPY = True
except ImportError:
    HAS_CUPY = False

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

class GPUAcceleratedGenerator:
    """Class for GPU-accelerated data generation"""
    
    def __init__(self, use_gpu: bool = True):
        self.use_gpu = False
        self.xp = None
        
        if use_gpu:
            try:
                import cupy as cp
                cp.cuda.Device(0).use()
                test_array = cp.array([1, 2, 3])
                test_result = cp.sum(test_array).get()
                
                self.use_gpu = True
                self.xp = cp
                
            except (ImportError, Exception) as e:
                self.use_gpu = False
                try:
                    import numpy as np
                    self.xp = np
                except ImportError:
                    self.xp = None
    
    def generate_batch_int(self, batch_size: int, num_fields: int, 
                          max_bits: int = 32) -> List[List[int]]:
        """Generate batch of integer values"""
        if not self.xp:
            # Fallback to pure Python
            try:
                max_val = (1 << max_bits) - 1
                return [[random.randint(0, max_val) 
                        for _ in range(num_fields)] 
                       for _ in range(batch_size)]
            except Exception as e:
                # Ultimate fallback with safe values
                return [[random.randint(0, 1000) 
                        for _ in range(num_fields)] 
                       for _ in range(batch_size)]
        
        try:
            max_val = (1 << max_bits) - 1
            
            batch = self.xp.random.randint(0, max_val, (batch_size, num_fields))
            
            if self.use_gpu:
                try:
                    return batch.get().tolist()
                except Exception as e:
                    # Fallback if GPU memory transfer fails
                    self.use_gpu = False
                    self.xp = np if HAS_NUMPY else None
                    return self.generate_batch_int(batch_size, num_fields, max_bits)
            else:
                return batch.tolist()
                
        except Exception as e:
            # Complete fallback to Python
            self.use_gpu = False
            self.xp = None
            try:
                max_val = (1 << max_bits) - 1
                return [[random.randint(0, max_val) 
                        for _ in range(num_fields)] 
                       for _ in range(batch_size)]
            except Exception:
                # Ultimate safe fallback
                return [[random.randint(0, 1000) 
                        for _ in range(num_fields)] 
                       for _ in range(batch_size)]

    def generate_batch_float(self, batch_size: int, num_fields: int) -> List[List[float]]:
        """Generate batch of float values"""
        if not self.xp:
            # Fallback to pure Python
            try:
                return [[random.random() for _ in range(num_fields)] 
                       for _ in range(batch_size)]
            except Exception as e:
                # Ultimate fallback with safe values
                return [[0.5 for _ in range(num_fields)] 
                       for _ in range(batch_size)]
        
        try:
            batch = self.xp.random.random((batch_size, num_fields))
            
            if self.use_gpu:
                try:
                    return batch.get().tolist()
                except Exception as e:
                    # Fallback if GPU memory transfer fails
                    self.use_gpu = False
                    self.xp = np if HAS_NUMPY else None
                    return self.generate_batch_float(batch_size, num_fields)
            else:
                return batch.tolist()
                
        except Exception as e:
            # Complete fallback to Python
            self.use_gpu = False
            self.xp = None
            try:
                return [[random.random() for _ in range(num_fields)] 
                       for _ in range(batch_size)]
            except Exception:
                # Ultimate safe fallback
                return [[0.5 for _ in range(num_fields)] 
                       for _ in range(batch_size)]