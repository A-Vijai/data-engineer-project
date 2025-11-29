# debug_popen_full.py
import subprocess, sys, os, traceback, shutil, time

LOG = os.path.join(os.getcwd(), "popen_debug.log")

_real_popen = subprocess.Popen

def safe_repr(x):
    try:
        return repr(x)
    except Exception:
        return "<unreprable>"

def log(msg):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} {msg}\n"
    with open(LOG, "a", encoding="utf8") as f:
        f.write(line)
    print(line, end="")

def patched_popen(*popen_args, **popen_kwargs):
    cmd = popen_args[0] if popen_args else popen_kwargs.get("args")
    try:
        # Build summary
        t = type(cmd).__name__
        if isinstance(cmd, (list, tuple)):
            first = cmd[0] if len(cmd) else "<empty>"
            summary = f"LIST len={len(cmd)} first={safe_repr(first)}"
        else:
            first = cmd
            summary = f"STR first={safe_repr(first)[:400]}"
        # Try to resolve first token to an absolute path
        resolved = None
        if isinstance(first, str):
            # if first looks like an absolute path, report exists; otherwise try shutil.which
            if os.path.isabs(first) and os.path.exists(first):
                resolved = first
                exists = True
            else:
                resolved = shutil.which(first)
                exists = bool(resolved and os.path.exists(resolved))
        else:
            exists = False

        env_preview = None
        env = popen_kwargs.get("env", os.environ)
        env_preview = env.get("PATH", "")[:800].replace(os.pathsep, ";")

        log(f"POPEN CALLED: type={t} summary={summary}")
        log(f"  cwd={popen_kwargs.get('cwd', os.getcwd())}")
        log(f"  first_token_resolved={safe_repr(resolved)} exists={exists}")
        log(f"  env PATH preview (truncated): {env_preview}")
        log(f"  full cmd repr (truncated): {safe_repr(cmd)[:1000]}")
    except Exception as ex:
        log(f"POPEN LOGGING FAILED: {ex}")

    # call the real Popen and if it raises we capture and re-raise after logging
    try:
        return _real_popen(*popen_args, **popen_kwargs)
    except Exception as e:
        log(f"POPEN RAISED: {type(e).__name__} {e}")
        # include traceback
        tb = traceback.format_exc()
        log(f"POPEN TRACEBACK: {tb[:2000]}")
        raise

# install monkeypatch
subprocess.Popen = patched_popen

# basic environment info
log("=== START debug_popen_full.py ===")
log(f"Python executable: {sys.executable}")
log(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")
log(f"HADOOP_HOME: {os.environ.get('HADOOP_HOME')}")
log(f"where java: {shutil.which('java')}")
log(f"where winutils: {shutil.which('winutils.exe')}")
log(f"CWD: {os.getcwd()}")

# attempt to start Spark
from pyspark.sql import SparkSession
log("Attempting to create SparkSession...")
try:
    spark = SparkSession.builder.master("local[1]").appName("debug_full").getOrCreate()
    log("Spark started: version=" + str(spark.version))
    spark.stop()
    log("Spark stopped normally")
except Exception as e:
    log(f"SPARK START FAILED: {type(e).__name__} {e}")
    log("Full traceback:")
    tb = traceback.format_exc()
    log(tb)
    # keep the file for inspection
    print("\n--- Debug log written to", LOG, "---\n")
    raise
finally:
    log("=== END debug_popen_full.py ===")
