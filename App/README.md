# Installation
1. Install postgres and configure the password and user in config file (database: stackoverflowdatabase user: dbuser )
2. Run spark cluster to import data into database.
3. Install neccessary library:
     `pip3 install psycopg2-binary`
     `pip3 install networkx`
4. Setup ufw and ngnix as proxy for routing http to server. (need domain name)
5. Setup service called dash.service and run `sudo systemctl start dash.service` [template file](https://github.com/Shawn5141/Insight-DE-Project-Stack-Community-Analysis/tree/master/DevOp/template). Instruction document: [link](https://www.digitalocean.com/community/tutorials/how-to-serve-flask-applications-with-gunicorn-and-nginx-on-ubuntu-18-04)
6. Run on local: python app.py and use address shown on terminal.

The UI provide analysis of Stack Overflow Post data to find out network between tags

![UI](../img/UI.PNG)
