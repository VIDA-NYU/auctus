FROM mediagis/nominatim:3.3

# Increase memory limit
RUN find /etc -name php.ini -exec sed -i '/^memory_limit *= *[0-9]/ c memory_limit = 1024M' {} ';'
RUN find /app -name \*.php\* -exec sed -i "s/ini_set('memory_limit', *'[0-9]\+M');/ini_set('memory_limit', '1024M');/" {} ';'

# Enable batch mode
RUN bash -c "cd /app/src/build && echo $'--- settings/settings.php\\n\
+++ settings/settings.php\\n\
@@ -106 +106 @@\\n\
-@define(\\'CONST_Search_BatchMode\\', false);\\n\
+@define(\\'CONST_Search_BatchMode\\', true);\\n\
' | patch -p0"

# Print apache log as well as postgres log
RUN sed -i '/tail -f/ a tail -f /var/log/apache2/error.log &' start.sh
