# verify_spark.py
import os
import sys
import shutil
import traceback
import subprocess
from pyspark.sql import SparkSession

def pre_checks():
    print("Python executable:", sys.executable)
    print("OS:", os.name, sys.platform)
    java_home = os.environ.get("JAVA_HOME")
    hadoop_home = os.environ.get("HADOOP_HOME")
    print("JAVA_HOME:", java_home)
    print("HADOOP_HOME:", hadoop_home)
    print("where java:", shutil.which("java"))
    print("where winutils:", shutil.which("winutils.exe"))

    # Validate java
    java_path = shutil.which("java")
    if not java_path:
        print("\nERROR: 'java' not found on PATH. Install a JDK and set JAVA_HOME, then add %JAVA_HOME%\\bin to PATH.")
        return False

    # Validate winutils on Windows
    if os.name == "nt":
        if not hadoop_home:
            print("\nERROR: HADOOP_HOME is not set. Set HADOOP_HOME to the parent folder that contains 'bin' (example: C:\\Hadoop).")
            return False
        expected = os.path.join(hadoop_home, "bin", "winutils.exe")
        if not os.path.exists(expected):
            print(f"\nERROR: winutils.exe not found at expected location: {expected}")
            print("Place winutils.exe into %HADOOP_HOME%\\bin and ensure %HADOOP_HOME%\\bin is on PATH.")
            return False

    return True

def ensure_local_env_precedence():
    """
    Ensure the current Python process sees the desired java first on PATH.
    This does not change system environment variables — only the current process.
    """
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        java_bin = os.path.join(java_home, "bin")
        # Prepend to process PATH if not already first
        cur_path = os.environ.get("PATH", "")
        parts = cur_path.split(os.pathsep)
        if parts and parts[0].lower() != java_bin.lower():
            # remove any existing occurrences of this path and re-prepend
            parts = [p for p in parts if p and p.lower() != java_bin.lower()]
            parts.insert(0, java_bin)
            os.environ["PATH"] = os.pathsep.join(parts)
            print(f"Prepending {java_bin} to PATH for this process.")

def monkeypatch_ver_call():
    """
    Returns (restore_fn) that will restore the original subprocess.Popen when called.
    This patch only intercepts the specific case where code attempts to Popen('ver') without shell=True.
    """
    if os.name != "nt":
        # no-op on non-windows
        return lambda: None

    _real_popen = subprocess.Popen

    def _patched_popen(*popen_args, **popen_kwargs):
        cmd = popen_args[0] if popen_args else popen_kwargs.get("args")
        # Intercept exact string "ver" (case-insensitive) to force cmd.exe /c ver
        if isinstance(cmd, str) and cmd.strip().lower() == "ver":
            cmd_exe = os.environ.get("COMSPEC", r"C:\Windows\System32\cmd.exe")
            new_args = [cmd_exe, "/c", "ver"]
            # Remove shell kwarg if present (we pass a list)
            if "shell" in popen_kwargs:
                popen_kwargs = {k: v for k, v in popen_kwargs.items() if k != "shell"}
            return _real_popen(new_args, **popen_kwargs)
        return _real_popen(*popen_args, **popen_kwargs)

    subprocess.Popen = _patched_popen

    def _restore():
        subprocess.Popen = _real_popen

    return _restore

def run_verify():
    # 1) pre-checks
    ok = pre_checks()
    if not ok:
        sys.exit(2)

    # 2) make sure current process sees JAVA_HOME\bin first (non-destructive)
    ensure_local_env_precedence()

    # 3) monkey-patch subprocess.Popen for the 'ver' shell-built-in issue on Windows
    restore_popen = monkeypatch_ver_call()

    # 4) print a small file listing of hadoop bin (diagnostic)
    hhome = os.environ.get("HADOOP_HOME")
    if hhome:
        binpath = os.path.join(hhome, "bin")
        try:
            print("Files in HADOOP_HOME\\bin:", os.listdir(binpath))
        except Exception as e:
            print("Could not list HADOOP_HOME\\bin:", type(e).__name__, e)

    # 5) Attempt to start SparkSession
    print("\nAttempting to start SparkSession...\n")
    try:
        spark = SparkSession.builder \
            .appName("verify") \
            .master("local[1]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.python.worker.reuse", "false") \
            .getOrCreate()
        print("Spark version:", spark.version)
        spark.stop()
        print("Spark started + stopped successfully")
    except FileNotFoundError as e:
        print("FileNotFoundError caught:")
        print("  errno:", getattr(e, "errno", None))
        print("  filename:", getattr(e, "filename", None))
        print("  strerror:", getattr(e, "strerror", None))
        traceback.print_exc()
        raise
    except Exception as e:
        print("Other Exception caught:", type(e).__name__, e)
        traceback.print_exc()
        raise
    finally:
        # restore monkey-patch
        try:
            restore_popen()
        except Exception:
            pass

if __name__ == "__main__":
    run_verify()
