# COVIDashFlow

[![Made with Pthon](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org/)

A simple Apache Airflow project which works as the ETL of [COVIDash.it](https://www.covidash.it/)

## For developers
To see it in action have a MongoDB instance ready and populate the `.env` file with the appropriate value. Then 
```shell
docker-compose --env-file ./.env up
```
The Airflow Web UI will be listening at `http://0.0.0.0:8080`
It can be stopped with 
```shell
docker-compose down
```

## Donation
If you liked this project or if I saved you some time, feel free to buy me a beer. Cheers!

[![paypal](https://www.paypalobjects.com/en_US/IT/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=PMW6C23XTQDWG)
