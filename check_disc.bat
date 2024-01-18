@echo off
d:
call yd d:\intdaily\japan\jsda\jsda
del *.txt *.err
call dlxlist <ii >nul 
ren dlxlist.txt check_labels.txt


