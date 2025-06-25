@echo off
echo excuting test.sql ...
mysql -h 127.0.0.1 -P 9030 -u root -e "source test.sql"
pause 