
import os, glob, shutil, tempfile

def export_single_csv(df, dst_dir: str, filename: str):
    with tempfile.TemporaryDirectory() as tmp:
        (df.coalesce(1).write.option("header", True).mode("overwrite").csv(tmp))
        part = glob.glob(os.path.join(tmp, "part-*.csv"))[0]
        os.makedirs(dst_dir, exist_ok=True)
        shutil.move(part, os.path.join(dst_dir, filename))
