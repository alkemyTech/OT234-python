from datetime import datetime

def get_duration(func):
    def wrapper(*args, **kwargs):
        start = datetime.now()
        func(*args, **kwargs)
        end = datetime.now()
        time = end - start
        print('Took ', time.total_seconds(), ' s')
    return wrapper

        
        