import os
import sys

pathname = os.path.dirname(sys.argv[0])

full_path = '{}\\'.format(str(os.path.abspath(pathname)))
print(full_path)

main_directory_finder = [x[0] for x in os.walk(full_path)]
print(main_directory_finder)