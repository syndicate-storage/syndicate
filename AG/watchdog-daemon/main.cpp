#include <daemon-config.h>

int main(int argc, char* argv[]) {
    daemon_config *dc = get_daemon_config("watchdog.conf", NULL);
    cout<<"ag_daemon_port> "<<dc->ag_daemon_port<<endl;
    cout<<"watchdog_daemon_port> "<<dc->ag_daemon_port<<endl;
    cout<<"admin_email> "<<dc->ag_daemon_port<<endl;
    size_t i=0;
    for (i=0; i < dc->ag_list.size(); i++)
	cout<<"AG> "<<dc->ag_list[i]<<endl;
    exit(0);
}

