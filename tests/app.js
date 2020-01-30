const express = require('express')

const app1 = express()
const app1Port = 8080
app1.use(express.static('public'))
app1.listen(app1Port, () => console.log(`app1 listening on port ${app1Port}!`))

const app2 = express()
const app2Port = 8081
app2.get('/tests/*.js', (req, res) => {
    var id = req.originalUrl.split("/")[2].split(".")[0];
    res.header("Access-Control-Allow-Origin", "*");
    res.send('var ' + id + ' = true; console.log("' + id + '");')
})
app2.listen(app2Port, () => console.log(`app2 listening on port ${app2Port}!`))