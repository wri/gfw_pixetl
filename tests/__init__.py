# import os
# from typing import Optional
#
# from gfw_pixetl.utils import set_cwd
#
# CWD: Optional[str] = None
#
#
# def setup_module(module):
#     global CWD
#     CWD = set_cwd()
#     print('\nsetup_module()')
#
# def teardown_module(module):
#     os.chdir(os.path.dirname(os.path.curdir))
#     os.rmdir(CWD)
