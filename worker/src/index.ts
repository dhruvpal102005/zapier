import { PrismaClient } from "@prisma/client";
import { Kafka } from "kafkajs";

const TOPIC_NAME = "zap-events";

const client = new PrismaClient();

const kafka = new Kafka({
    clientId: 'outbox-processor-2',
    brokers: ['localhost:9092']
})

async function main() {
    const consumer = kafka.consumer({ groupId: 'main-worker-2' });
    await consumer.connect();

    await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });

    await consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                partition,
                offset: message.offset,
                value: message.value?.toString(),
            })

            const zapRunId = message.value?.toString();

            if (!zapRunId) {
                return;
            }

            const zapRunDetails = await client.zapRun.findFirst({
                where: {
                    id: zapRunId
                },
                include: {
                    zap: {
                        include: {
                            actions: {
                                include: {
                                    type: true
                                }
                            }
                        }
                    }
                }
            });

            const currentAction = zapRunDetails?.zap.actions.find(x => x.sortingOrder === 1);

            if (!currentAction) {
                console.log("No actions found");
                return;
            }

            console.log("Processing action:", currentAction);
            console.log("Action type:", currentAction.type.name);

            // Simulate processing time
            await new Promise(r => setTimeout(r, 500));

            const lastAction = zapRunDetails?.zap.actions.find(x => x.sortingOrder === 2);
            if (!lastAction) {
                console.log("No actions found");
                return;
            }

            console.log("Processing action:", lastAction);
            console.log("Action type:", lastAction.type.name);

            // Simulate processing time
            await new Promise(r => setTimeout(r, 500));

            console.log("processing done");

            await consumer.commitOffsets([{
                topic: TOPIC_NAME,
                partition: partition,
                offset: (parseInt(message.offset) + 1).toString()
            }])
        },
    })
}

main()