#include <daemon-configh>


daemon_config* get_daemon_config(char *cfg_file, char **argv) {
    daemon_config *cfg = (daemon_config*)malloc(sizeof(daemon_config));
    cfg->ag_list = NULL;
    cfg->ag_daemon_port = NULL;
    cfg->watchdog_daemon_port = NULL;
    cfg->adming_email = NULL;
    cfg->send_notification = false;
    cfg->start_daemon = false;
    parse_cmd_lint(argv, cfg);
    parse_daemon_config(cfg_file, cfg);
    return cfg;
}

void parse_daemon_config(char *cfg_file, daemon_config *cfg) {
}

void parse_cmd_line(char **argv, daemon_config *cfg) {
}

