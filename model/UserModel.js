const BaseModel = require('./BaseModel')
const _ = require('lodash')

class UserModel extends BaseModel {
    constructor() {
        super()
        this.TableName = 'TT_User'
    }
    // 主键查询
    async getUserById(inparam) {
        let query = { key: { userId: inparam.userId }, project: ['userName', 'userId', 'createAt'] }
        let res = await this.getItem(query)
        return res.Item
    }
    // 索引查询
    async queryUser(inparam) {
        // 索引查询
        // let query = {
        //     IndexName: 'userNameIndex',
        //     KeyConditionExpression: 'userName=:userName',
        //     ExpressionAttributeValues: {
        //         ':userName': inparam.userName
        //     }
        // }
        // 不指定索引 默认主分区查询
        // let query = {
        //     KeyConditionExpression: 'userId=:userId',
        //     ExpressionAttributeValues: {
        //         ':userId': inparam.userId
        //     }
        // }
        // 查询分页示例
        // let query = {
        //     IndexName: 'groupNameIndex',
        //     KeyConditionExpression: '#groupName=:groupName',
        //     ScanIndexForward: false,                            // 降序返回
        //     Limit: 20,                                          // 查询数量
        //     ExpressionAttributeNames: {
        //         '#groupName': 'groupName'
        //     },
        //     ExpressionAttributeValues: {
        //         ':groupName': inparam.groupName
        //     }
        // }
        // if (inparam.LastEvaluatedKey) {
        //     query.ExclusiveStartKey = inparam.LastEvaluatedKey
        // }

        let res = await this.query(query)
        return res.Items
    }
    // 扫描查询
    async scanUser(inparam) {
        let scan = {
            ProjectionExpression: '#userId,userName',
            FilterExpression: 'userName=:userName or #userId>:userId',
            ExpressionAttributeNames: {
                '#userId': 'userId'
            },
            ExpressionAttributeValues: {
                ':userName': inparam.userName,
                ':userId': inparam.userId
            }
        }
        let res = await this.scan(scan)
        return res.Items
    }
    // 分页查询
    async page() {
        // ScanIndexForward: false,  // 降序返回
        // Limit:20,                 // 查询数量
    }
    // 插入数据
    async putData(inparam) {
        let itemData = {
            userId: inparam.userId,
            userName: inparam.userName,
            createAt: inparam.createAt || Date.now()
        }
        let res = await this.putItem(itemData)
        return res
    }
    // 更新数据
    async updateData(inparam) {
        // 构造更新数据
        let updateItem = {
            key: { userId: inparam.userId },
            updateItem: { userName: inparam.userName }
        }
        // 调用数据库操作
        let res = await this.updateItem(updateItem)
        return res
    }
    // 删除数据
    async deleteData(inparam) {
        let deleteItem = {
            key: { userId: inparam.userId }
        }
        let res = await this.deleteItem(deleteItem)
        return res
    }
    // 批量写入数据 一次性最多能操作25个
    async batchWriteUser(inparam) {
        let batchArr = _.chunk(inparam, 25)
        for (let chunk of batchArr) {
            let batchData = {}
            batchData[this.TableName.TableName] = []
            //构造批量写入
            for (let item of chunk) {
                batchData[this.TableName.TableName].push({
                    PutRequest: {
                        Item: item
                    }
                })
            }
            // 构造批量删除
            // for (let item of chunk) {
            //     batchData[this.TableName.TableName].push({
            //         DeleteRequest: {
            //             Key: { userId: item.userId }
            //         }
            //     })
            // }
            // console.log(JSON.stringify(batchData))
            // return
            await this.batchWrite(batchData)
        }
    }


}

// 测试示例
async function run() {
    try {
        // let res = await new UserModel().getUserById({ userId: '11' })
        // let res = await new UserModel().queryUser({ userName: 'test50' })
        // let res = await new UserModel().queryUser({ userId: '50' })
        // let res = await new UserModel().queryUser({ groupName: 1, LastEvaluatedKey: { groupName: 1, userId: '61' } })
        // let res = await new UserModel().scanUser({ userId: '50', userName: 'test5' })
        // let res = await new UserModel().putData({ userId: '1111', userName: 'test02' })
        // let res = await new UserModel().updateData({ userId: '3', userName: 'test03' })
        // let res = await new UserModel().deleteData({ userId: '0' })
        // let data = [
        //     { action: 'update', tableName: 'TT_User', data: { userId: '0' }, updateItem: { userName: 'test0', groupName: 0 } },
        //     // { action: 'put', tableName: 'TT_User', data: { userId: '200', userName: `test200`, createAt: Date.now(), updateAt: Date.now(), groupName: _.sample([0, 1]), sex: _.sample([0, 1]) } }
        //     { action: 'delete', tableName: 'TT_User', data: { userId: '200' } },
        // ]
        // let res = await new BaseModel().transcatWrite(data)
        // let batchGetData = [
        //     { tableName: 'TT_User', key: { userId: '11' } ,project:['userId','userName']},
        //     { tableName: 'TT_User', key: { userId: '22' } ,project:['userId','userName']},
        //     { tableName: 'TT_Token', key: { userId: '11' } ,project:['aa','bb']}
        // ]
        // let res = await new BaseModel().batchGet(batchGetData)
        // let transcatData = [
        //     { tableName: 'TT_User', key: { userId: '11' } ,project:['userId','userName']},
        //     { tableName: 'TT_User', key: { userId: '22' } ,project:['userId','userName']},
        //     { tableName: 'TT_Token', key: { userId: '11' } ,project:['aa','bb']}
        // ]
        // let res = await new BaseModel().transcatGet(transcatData)
        let data = [
            { action: 'put', tableName: 'TT_User', data: { userId: '200', userName: `test200`, createAt: Date.now(), updateAt: Date.now(), groupName: _.sample([0, 1]), sex: _.sample([0, 1]) } },
            { action: 'put', tableName: 'TT_Token', data: { id: '111111111', token: `dsfedcswfefe`, createAt: Date.now(), updateAt: Date.now() } },
            { action: 'delete', tableName: 'TT_User', data: { userId: '200' } },
        ]
        let res = await new BaseModel().batchWrite(data)
        // let batchData = []
        // for (let i = 0; i < 100; i++) {
        //     batchData.push({ userId: i.toString(), userName: `test${i}`, createAt: Date.now(), updateAt: Date.now(),groupName:_.sample([0,1]),sex:_.sample([0,1]) })
        // }
        // let res = await new UserModel().batchWriteUser(batchData)
        console.log(1, res)


    } catch (err) {
        console.log(2, err)
        return err
    }

}
run()