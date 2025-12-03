import express from "express";
import { PrismaClient } from "@prisma/client";

const client = new PrismaClient();

const app = express();
app.use(express.json());

app.post("/hooks/catch/:userId/:zapId", async (req, res) => {
    const userId = req.params.userId;
    const zapId = req.params.zapId;
    const body = req.body;

    const zap = await client.zap.findUnique({
        where: {
            id: zapId
        }
    })

    if (!zap) {
        res.status(404).json({
            message: "Zap not found"
        })
        return
    }

    if (zap.userId !== parseInt(userId)) {
        res.status(403).json({
            message: "You don't have permission to access this zap"
        })
        return
    }

    // store in db a new trigger
    await client.$transaction(async tx => {
        const run = await tx.zapRun.create({
            data: {
                zapId: zapId,
                metadata: body
            }
        });

        await tx.zapRunOutbox.create({
            data: {
                zapRunId: run.id
            }
        })
    })
    res.json({
        message: "Webhook received"
    });
})


app.listen(3000);