var express = require('express');
var bodyParser = require('body-parser')
var pg = require('pg');
var cors = require('cors')

var app = express();

app.use(cors());
app.options('*', cors());

// parse application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: false }))

// parse application/json
app.use(bodyParser.json())

var connectionsCredentials = [];
var connections = [];

app.get('/', function(req, res){
    res.send("Hello world");
});

//QUERIES
app.post('/query', function(req, res){
    let id = 0;
    let sql = req.body.sql;
    if(connections.length  == 0)
        res.status(500).json("No connections availables");
    else{
        let client = connections[id];
        
        var query = client.query(sql, function(err, result){
            if(err){
                console.log(err);
                res.status(500).json(err);
            }else{
                res.status(200).json(result.rows);
            }
        });
        //query.on("err")
        // query.on("row", function (row, result) {
        //     result.addRow(row);
        // });
        // query.on("end", function (result) {
        //     res.status(200).json(result.rows);
        // });
    }
});

//CONNECTIONS
app.get('/connections',function(req, res){
    res.status(200).json(connectionsCredentials);
});

app.post('/connections',function(req, res){
    let credentials = req.body.credentials;

    let connString = `pg://${credentials.username}:${credentials.password}@${credentials.host}:${credentials.port}/${credentials.db}`;
    console.log(connString);
    // let connString = `pg://${credentials.username}:${credentials.password}@${credentials.host}:${credentials.port}`;
    let client = new pg.Client(connString);

    client.connect(function(err){
        if(err)
            res.status(500).json(err);
        else{
            credentials.password = '********';
            connectionsCredentials.push(credentials);
            connections.push(client);

            res.status(200).json("Connection added");    
        }
    });
});

app.put('/connections/:id',function(req, res){
    res.status(200).json("success");
});

app.delete('/connection:id',function(req,res){
    let id = req.params.id;
    if(id >= connections.length)
        res.status(500).json("Connection not found");
    else{
        let client = connections[id];
        client.end();
        connections.splice(id, 1);
        connectionsCredentials.splice(id, 1);
    }        
});

//SESSIONS
app.get("/sessions", function(req,res){
    if(connections.length  == 0)
        res.status(500).json("No connections availables");
    else{
        let client = connections[0];
        
        var query = client.query("select * from pg_stat_activity;");
        query.on("row", function (row, result) {
            result.addRow(row);
        });
        query.on("end", function (result) {
            res.status(200).json(result.rows);
        });
    }
});

//DATABASES
app.get("/connection/:id/databases", function(req, res){
    let id = 0;
    if(this.credentials.length == 0)
        res.status(500).json("No connections availables");
    else{
        let cred = this.credentials[id];
        let client = this.connections[id];

        let sql = `select datname from pg_database pd inner join pg_user pu on pd.datdba = pu.usesysid where pu.usename = ${cred.username}`;

        client.query(sql, function(err, result){
            if(err){
                res.status(500).json(err);
            }else{
                res.status(200).json(result.rows);
            }
        });
    }
});

app.post("/connection/:id/databases/:dbname", function(req, res){
    let id = 0;
    let dbname = req.params.dbname;
    if(this.credentials.length == 0)
        res.status(500).json("No connections availables");
    else{
        let cred = this.credentials[id];
        let client = this.connections[id];

        let sql = `create database ${dbname} owner ${cred.username}`;

        client.query(sql, function(err, result){
            if(err){
                res.status(500).json(err);
            }else{
                console.log(result);
                res.status(200).json('Database created');
            }
        });
    }
});

//TABLES
app.get('/connection/:id/tables', function(req, res){
    let id = 0;
    let dbname = req.params.dbname;
    if(this.credentials.length == 0)
        res.status(500).json("No connections availables");
    else{
        let cred = this.credentials[id];
        let client = this.connections[id];

        let sql = `select tablename from pg_tables where owner = ${cred.username}`;

        client.query(sql, function(err, result){
            if(err){
                res.status(500).json(err);
            }else{
                console.log(result);
                res.status(200).json('Database created');
            }
        });
    }
});

//FUNCTIONS
app.get('/connection/:id/functions/:fn_name/ddl', function(req, res){
    let id = 0;
    let fn_name = req.params.fn_name;
    if(this.credentials.length == 0)
        res.status(500).json("No connections availables");
    else{
        let cred = this.credentials[id];
        let client = this.connections[id];

        let sql = `select fn_get_function_definition(${fn_name});`;

        client.query(sql, function(err, result){
            if(err){
                res.status(500).json(err);
            }else{
                res.status(200).json(result.rows);
            }
        });
    }
});

app.listen(8000);