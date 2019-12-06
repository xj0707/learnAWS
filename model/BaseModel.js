const AWS = require('aws-sdk')

AWS.config.update({ region: 'ap-northeast-1' })

const docClient = new AWS.DynamoDB.DocumentClient({ apiVersion: '2012-08-10' })


module.exports = class BaseModel {
    constructor() {
        this.baseitem = {
            createAt: Date.now(),
            updateAt: Date.now()
        }
    }
    /**
     * 操纵数据库
     * @param {String} action 
     * @param {Object} params 
     */
    db$(action, params) {
        return docClient[action](params).promise()
    }
    /**
     * 主键查询
     * @param {object} conditions 
     * {key:{userId:'111'},project:['userName','userId','createAt']}
     */
    getItem(conditions = {}) {
        let otps = this.bindProjectParms(conditions.project)
        let params = {
            TableName: item.tableName || this.TableName,
            ConsistentRead: true,
            Key: conditions.key,
            ...otps
        }
        return this.db$('get', params)
    }
    /**
     * 插入单项数据
     * @param {Object} conditions 
     * {userId:'11',userName:'test01'}
     */
    putItem(conditions = {}) {
        let params = {
            TableName: item.tableName || this.TableName,
            Item: {
                ...this.baseitem,
                ...conditions
            }
        }
        return this.db$('put', params)
    }
    /**
     * 更新一条数据
     * @param {object} conditions 
     * {key:{userId:'111'},updateItem:{p1:v1,p2:v2}}
     */
    updateItem(conditions = {}) {
        let otps = this.bindSetParms(conditions.updateItem)
        let params = {
            TableName: item.tableName || this.TableName,
            Key: conditions.key,
            ...otps
        }
        return this.db$('update', params)
    }
    /**
     * 删除一条数据
     * @param {*} conditions 
     * {key:{userId:'11'}}
     */
    deleteItem(conditions = {}) {
        let params = {
            TableName: item.tableName || this.TableName,
            Key: conditions.key
        }
        return this.db$('delete', params)
    }
    /**
     * 使用索引查询
     * @param {Object} conditions 
     * {}
     */
    query(conditions = {}) {




        let params = {
            ...this.TableName,
            ...conditions
        }
        return this.queryInc(params, null)
    }
    // 查询结果超过1M的情况需要递归查询
    queryInc(params, result) {
        return this.db$('query', params).then((res) => {
            if (!result) {
                result = res
            } else {
                result.Items.push(...res.Items)
            }
            if (res.LastEvaluatedKey) {
                params.ExclusiveStartKey = res.LastEvaluatedKey
                return this.queryInc(params, result)
            } else {
                return result
            }
        }).catch((err) => {
            console.error(err)
            return err
        })
    }
    // 全表扫描查询
    scan(conditions = {}) {
        let params = {
            ...this.TableName,
            ...conditions
        }
        return this.scanInc(params, null)
    }
    // 当扫描数据超过1M的时候递归查询
    scanInc(params, result) {
        return this.db$('scan', params).then((res) => {
            if (!result) {
                result = res
            } else {
                result.Items.push(...res.Items)
            }
            if (res.LastEvaluatedKey) {
                params.ExclusiveStartKey = res.LastEvaluatedKey
                return this.queryInc(params, result)
            } else {
                return result
            }
        }).catch((err) => {
            console.error(err)
            return err
        })
    }
    /**
     * 一个表或多个表通过主键查询多条数据（最多100条同时查询）
     * @param {Array} conditions 
     * [
     * {tableName:'tableName1',key:{userId:'111'},project:['userId','userName']} 
     * {tableName:'tableName2',key:{userId:'222'},project:['aa','bb']} 
     * ]
     */
    batchGet(conditions = []) {
        let params = { RequestItems: {} }
        for (let item of conditions) {
            let tableName = item.tableName || this.TableName
            if (params.RequestItems[tableName]) {
                params.RequestItems[tableName].Keys.push(item.key)
            } else {
                params.RequestItems[tableName] = { Keys: [item.key], ConsistentRead: true }
                if (item.project.length > 0) {
                    let otps = this.bindProjectParms(item.project)
                    Object.assign(params.RequestItems[tableName], otps)
                }
            }
        }
        return this.db$('batchGet', params)
    }
    /**
     * 一个或多个表 批量删除或写入 （最多25条同时操作）
     * @param {Array} conditions 
     * [
     * {action:'put',tableName:'tableName',data:{userId:'111',userName:'userName'}}
     * {action:'delete',tableName:'tableName',data:{userId:'111'}}
     * ]
     */
    batchWrite(conditions = []) {
        let params = { RequestItems: {} }
        for (let item of conditions) {
            let actionItem = {}
            switch (item.action) {
                case 'put':
                    actionItem = {
                        PutRequest: {
                            Item: item.data
                        }
                    }
                    break;
                case 'delete':
                    actionItem = {
                        DeleteRequest: {
                            Key: item.data
                        }
                    }
                    break;
                default:
                    break;
            }
            let tableName = item.tableName || this.TableName
            if (params.RequestItems[tableName]) {
                params.RequestItems[tableName].push(actionItem)
            } else {
                params.RequestItems[tableName] = [actionItem]
            }
        }
        // console.log(JSON.stringify(params))
        return this.db$('batchWrite', params)
    }

    /**
     * 事务同步操作 更新 写入 删除等操作 （最多同时10个操作）
     * @param {Array} conditions 
     * [
     *  {action:'put',tableName:'tableName',data:{userId:1,userName:2,createAt:Date.now()}}
     *  {action:'delete',tableName:'tableName',data:{userId:1}}
     *  {action:'update',tableName:'tableName',data:{userId:1},updateItem:{parms1:value1,parms2:value2}}
     * ]
     */
    transcatWrite(conditions = []) {
        let params = { TransactItems: [] }
        for (let item of conditions) {
            let tableName = item.tableName || this.TableName
            switch (item.action) {
                case 'put':
                    params.TransactItems.push({
                        Put: {
                            TableName: tableName,
                            Item: item.data
                        }
                    })
                    break;
                case 'delete':
                    params.TransactItems.push({
                        Delete: {
                            TableName: tableName,
                            Key: item.data
                        }
                    })
                    break;
                case 'update':
                    let otps = this.bindSetParms(item.updateItem)
                    params.TransactItems.push({
                        Update: {
                            TableName: tableName,
                            Key: item.data,
                            ...otps
                        }
                    })
                    break;
                default:
                    break;
            }
        }
        // console.log(JSON.stringify(params))
        return this.db$('transactWrite', params)
    }
    /**
     * 事务从一个表或多个表用原子方式查询项目
     * @param {Array} conditions
     * [
     * {tableName:'tableName1',key:{userId:'111'},project:['userId','userName']} 
     * {tableName:'tableName2',key:{userId:'222'},project:['aa','bb']} 
     * ]
     */
    transcatGet(conditions = []) {
        let params = { TransactItems: [] }
        for (let item of conditions) {
            let tableName = item.tableName || this.TableName
            let getItem = {
                Get: {
                    TableName: tableName,
                    Key: item.key
                }
            }
            if (item.project.length > 0) {
                let otps = this.bindProjectParms(item.project)
                Object.assign(getItem.Get, otps)
            }
            params.TransactItems.push(getItem)
        }
        return this.db$('transactGet', params)
    }
    /**
     *  构造更新参数
     * @param {Object} conditions 
     * 
     */
    bindSetParms(conditions = {}) {
        let keys = Object.keys(conditions), opts = {}
        if (keys.length > 0) {
            opts.UpdateExpression = 'set '
            opts.ExpressionAttributeValues = {}
            opts.ExpressionAttributeNames = {}
        }
        keys.forEach((k) => {
            opts.UpdateExpression += `#${k} = :${k},`
            opts.ExpressionAttributeNames[`#${k}`] = k
            opts.ExpressionAttributeValues[`:${k}`] = conditions[k]
        })
        opts.UpdateExpression = opts.UpdateExpression.substring(0, opts.UpdateExpression.length - 1)
        return opts
    }
    /**
     * 构造查询字段参数
     * @param {*} conditions
     *  
     */
    bindProjectParms(conditions = []) {
        let opts = {}
        if (conditions.length > 0) {
            opts.ProjectionExpression = ''
            opts.ExpressionAttributeNames = {}
            conditions.forEach((k) => {
                opts.ProjectionExpression += `#${k},`
                opts.ExpressionAttributeNames[`#${k}`] = k
            })
            opts.ProjectionExpression = opts.ProjectionExpression.substring(0, opts.ProjectionExpression.length - 1)
        }
        return opts
    }
    // 构造索引或扫描查询参数
    bindQueryParms() {

    }

}