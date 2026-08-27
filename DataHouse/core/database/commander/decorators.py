from functools import wraps

def exec_many(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        command, parameters, execute = func(self, *args, **kwargs)
        if execute:
            with self.conn:
                try:
                    self.conn.executemany(
                        command,
                        parameters
                    )
                except Exception as err:
                    print(command)
                    print(parameters)
                    raise err
        return command, parameters
    return wrapper

def exec_lines(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        commands, execute = func(self, *args, **kwargs)
        if execute:
            for cmd in commands:
                with self.conn:
                    try:
                        self.conn.execute(cmd)
                    except Exception as err:
                        print(commands)
                        print(cmd)
                        raise err
        print(commands)
        return commands
    return wrapper

def exec_line(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        command, execute = func(self, *args, **kwargs)
        if execute:
            with self.conn:
                try:
                    self.conn.execute(command)
                except Exception as err:
                    print(command)
                    raise err
        print(command)
        return command
    return wrapper

def eval_line(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        command, execute = func(self, *args, **kwargs)
        if execute:
            try:
                return self.conn.execute(command)
            except Exception as err:
                print(command)
                raise err
        else:
            return command
    return wrapper