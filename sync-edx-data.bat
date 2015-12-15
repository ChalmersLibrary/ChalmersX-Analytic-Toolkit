@echo off
aws s3 sync --exclude "*" --include "chalmersx*" s3://course-data/ ./course-data/
aws s3 sync s3://edx-course-data/chalmersx ./edx-course-data/
xcopy . Z:\edX\ /s /e /d /y