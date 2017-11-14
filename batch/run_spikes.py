
import datetime
import subprocess
import sys

log_file_path = '../logs/%s.log' % datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
f = open(log_file_path, 'w')

p = subprocess.Popen('python generate_aggr_data.py', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
for line in p.stdout.readlines():
    f.write(line + "\n")
retval = p.wait()
if retval != 0:
    f.write("generate_aggr_data.py failed : %d" % retval)
    f.close()
    sys.exit()
print "generate_aggr_data.py completed"

p = subprocess.Popen('python import_all.py', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
for line in p.stdout.readlines():
    f.write(line + "\n")
retval = p.wait()
if retval != 0:
    f.write("import_all.py failed : %d" % retval)
    f.close()
    sys.exit()
print "import_all.py completed"

p = subprocess.Popen('python predict.py', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
for line in p.stdout.readlines():
    f.write(line + "\n")
retval = p.wait()
if retval != 0:
    f.write("predict.py failed : %d" % retval)
    f.close()
    sys.exit()
print "predict.py completed"

p = subprocess.Popen('python import.py', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
for line in p.stdout.readlines():
    f.write(line + "\n")
retval = p.wait()
if retval != 0:
    f.write("import.py failed : %d" % retval)
    f.close()
    sys.exit()
print "import.py completed"

p = subprocess.Popen('python compare.py', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
for line in p.stdout.readlines():
    f.write(line + "\n")
retval = p.wait()
if retval != 0:
    f.write("compare.py failed : %d" % retval)
    f.close()
    sys.exit()
print "compare.py completed"

p = subprocess.Popen('python spikes.py', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
for line in p.stdout.readlines():
    f.write(line + "\n")
retval = p.wait()
if retval != 0:
    f.write("spikes.py failed : %d" % retval)
    f.close()
    sys.exit()
print "spikes.py completed"

f.close()
