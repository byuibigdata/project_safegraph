# https://www.nylas.com/blog/making-use-of-environment-variables-in-python/
# https://www.twilio.com/blog/environment-variables-python
# https://www.twilio.com/blog/environment-variables-python

# The load_dotenv() function will look for a file named .env in the current directory and will add all the variable definitions in it to the os.environ dictionary. If a .env file is not found in the current directory, then the parent directory is searched for it. The search keeps going up the directory hierarchy until a .env file is found or the top-level directory is reached.

# SAFEGRAPH_KEY=LNNmQ
# GITHUB_PAT=ghp_1y
# I save my .env file just outside my git repo hierarchy
# %%
# import sys
# !{sys.executable} -m pip install python-dotenv

# %%
import os
from dotenv import load_dotenv
load_dotenv()
sfkey = os.environ.get("SAFEGRAPH_KEY")

# %%
