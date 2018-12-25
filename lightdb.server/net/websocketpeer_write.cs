using LightDB.SDK;
using LightDB;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace LightDB.Server
{
    public partial class websockerPeer : lightchain.httpserver.httpserver.IWebSocketPeer
    {

        class DBLockObj
        {

        }
        DBLockObj dblock = new DBLockObj();
        public async Task OnDB_Write(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.write.back");
            msg.Params["_id"] = id;
            try
            {
                var data = msgRecv.Params["taskdata"];
                var signdata = msgRecv.Params["signdata"];
                var writetask = WriteTask.FromRaw(data);
                if (writetask.extData?.ContainsKey("lasthash") == true)
                {
                    var lastblockhashRecv = writetask.extData["lasthash"];
                    lock (dblock)
                    {
                        using (var snap = Program.storage.maindb.UseSnapShot())
                        {
                            var blockidlast = BitConverter.GetBytes((UInt64)(snap.DataHeight - 1));

                            byte[] taskhash = Helper.CalcHash256(data);
                            //不够用，还需要block高度
                            var lasthashFind = snap.GetValue(StorageService.tableID_BlockID2Hash, blockidlast).value;

                            if (Helper.BytesEquals(lastblockhashRecv, lasthashFind))
                            {
                                //数据追加处理
                                Action<WriteTask, byte[], IWriteBatch> afterparser = (_task, _data, _wb) =>
                                {
                                    if (_wb.snapshot.DataHeight != snap.DataHeight)
                                    {
                                        throw new Exception("sync problem,diff snap found.");
                                    }
                                    _wb.Put(StorageService.tableID_BlockID2Hash, snap.DataHeightBuf, DBValue.FromValue(DBValue.Type.Bytes, taskhash));
                                };
                                //写入数据
                                Program.storage.maindb.Write(writetask, afterparser);

                                //进表
                                msg.Params["blockid"] = snap.DataHeightBuf;
                                msg.Params["blockhash"] = taskhash;
                            }
                            else
                            {
                                msg.Params["_error"] = "block hash is error".ToBytes_UTF8Encode();
                            }

                        }
                    }
                }
                else
                {
                    msg.Params["_error"] = "no last block hash.".ToBytes_UTF8Encode();
                }

            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }

            //这个完全可以不要等待呀
            SendToClient(msg);

        }

    }
}
