import atexit
import os
import signal
import sys
from functools import wraps


def daemonize(pidfile, *, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null', exit_callback=None, **kw_args):
    """装饰器，使用守护进程运行函数"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if os.path.exists(pidfile):
                raise RuntimeError('Already running')

            try:
                if os.fork() > 0:
                    raise SystemExit(0)
            except OSError:
                raise RuntimeError('fork #1 failed.')

            os.setsid()

            try:
                if os.fork() > 0:
                    raise SystemExit(0)
            except OSError:
                raise RuntimeError('fork #2 failed.')

            sys.stdout.flush()
            sys.stderr.flush()

            with open(stdin, 'rb', 0) as f:
                os.dup2(f.fileno(), sys.stdin.fileno())
            with open(stdout, 'ab', 0) as f:
                os.dup2(f.fileno(), sys.stdout.fileno())
            with open(stderr, 'ab', 0) as f:
                os.dup2(f.fileno(), sys.stderr.fileno())

            with open(pidfile, 'w') as f:
                print(os.getpid(), file=f)

            if exit_callback is None:
                atexit.register(lambda: os.remove(pidfile))
            else:
                def callback():
                    os.remove(pidfile)
                    exit_callback(**kw_args)

                atexit.register(callback)

            def sigterm_handler(sig_no, frame):
                raise SystemExit(1)

            signal.signal(signal.SIGTERM, sigterm_handler)

            return func(*args, **kwargs)

        return wrapper

    return decorator


def kill_pid(pidfile):
    with open(pidfile, 'rt') as f:
        os.kill(int(f.read()), signal.SIGTERM)


def print_message(message, *, hint=True, **kwargs):
    if hint:
        print(message, **kwargs)


def run_backend(pidfile, *, hint=True, **deco_kwargs):
    def decorator(func):
        @wraps(func)
        def wrapper(*func_args, **func_kwargs):
            if len(sys.argv) != 2:
                print(f'程序使用方法: {sys.argv[0]} [start|stop]', file=sys.stderr)
                raise SystemExit(1)

            if sys.argv[1] == 'start':
                try:
                    print_message('启动程序', hint=hint)
                    return daemonize(pidfile, **deco_kwargs)(func)(*func_args, **func_kwargs)
                except RuntimeError as e:
                    print(f'启动失败，失败原因：{e}', file=sys.stderr)
                    raise SystemExit(1)
            elif sys.argv[1] == 'stop':
                try:
                    print_message('关闭程序', hint=hint)
                    kill_pid(pidfile)
                except FileNotFoundError:
                    print(f'关闭失败, 请检查{pidfile}是否存在', file=sys.stderr)
                    raise SystemExit(1)
            else:
                print(f'未知参数: {sys.argv[1]}', file=sys.stderr)
                raise SystemExit(1)

        return wrapper

    return decorator
