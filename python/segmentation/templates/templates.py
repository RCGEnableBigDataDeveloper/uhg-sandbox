class Template:
    chown = "hadoop fs -chown {0}{1}.db/{2}"
    chgrp = "hadoop fs -chgrp {0}:{1} {2}{3}.db/{4}"
    grant_select = "GRANT SELECT ON TABLE {0} TO USER {1}; "