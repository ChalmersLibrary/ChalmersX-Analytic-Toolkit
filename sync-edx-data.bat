@echo off
if [%1] == [] GOTO error
if [%2] == [] GOTO error
aws s3 sync --exclude "*" --include "%1*" s3://course-data/ ./course-data/
aws s3 sync s3://edx-course-data/%1 ./edx-course-data/
xcopy . %2 /s /e /d /y
GOTO end
:error
echo You have to submit organization and backup location as arguments.
:end