

const RPC_QUEUE_NAME = 'rpc_queue';
const amqp = require('amqplib');
const demo = require('./demo');
const _ = require('underscore');

let conn = null;
amqp.connect('amqp://guest:guest@localhost:5672').then( (c) => {

    conn = c;
    return conn.createChannel();
}).then( channel => {
    

    const inputs = [...Array(10).keys()];
    const outputs = [];
    return new Promise( (resolve, reject) => {
        const doUntil = (index) => 
        {
            if(inputs[index] === undefined)
            {
                conn.close();
                resolve(outputs);
            }else
            {
                channel.assertQueue('', { exclusive:true}).then( (raq) => {
                    const correlationId = generateUuid();
                    const d = demo.encodeDtoContext({name:'hello wfforld', age:10, n: inputs[index]});

                    channel.consume(raq.queue, function(msg) 
                    {
                        if (msg.properties.correlationId === correlationId) 
                        {
                            outputs.push(parseInt(msg.content.toString()));
                            doUntil(index + 1);

                        }
                    }, {
                        noAck: true
                    });

                    channel.sendToQueue(RPC_QUEUE_NAME,
                        Buffer.from(d), {
                            correlationId: correlationId,
                            replyTo: raq.queue
                        });

                });

            }
        };
        doUntil(0)


    });
    
}).then( outputs => {

    _.forEach(outputs, (o) => {

        console.log(o);
    });

});


function generateUuid() 
{
    return Math.random().toString() +  Math.random().toString() +  Math.random().toString();
}