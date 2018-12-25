using LightDB.SDK;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using LightDB;
namespace LightDB.Server
{

    public partial class websockerPeer : lightchain.httpserver.httpserver.IWebSocketPeer
    {
        System.Net.WebSockets.WebSocket websocket;

        public websockerPeer(System.Net.WebSockets.WebSocket websocket)
        {
            this.websocket = websocket;
            this.peerSnapshots = new System.Collections.Concurrent.ConcurrentDictionary<ulong, ISnapShot>();
            this.peerItertors = new System.Collections.Concurrent.ConcurrentDictionary<ulong, IEnumerator<byte[]>>();
        }
        public async Task OnConnect()
        {
            Console.CursorLeft = 0;

            Console.WriteLine("websocket in:" + websocket.SubProtocol);
        }

        public async Task OnDisConnect()
        {
            Console.CursorLeft = 0;

            Console.WriteLine("websocket gone:" + websocket.CloseStatus + "." + websocket.CloseStatusDescription);
            DisposeSnapShots();
        }
        public void DisposeSnapShots()
        {
            try
            {
                foreach (var psnap in peerSnapshots.Values)
                {
                    if (psnap != null)
                    {
                        try
                        {
                            psnap.Dispose();
                        }
                        catch
                        {

                        }
                    }
                }

                peerSnapshots.Values.Clear();
            }
            catch
            {

            }
        }
        private async Task SendToClient(NetMessage msg)
        {
            await this.websocket.SendAsync(msg.ToBytes(), System.Net.WebSockets.WebSocketMessageType.Binary, true, System.Threading.CancellationToken.None);
        }
        public async Task OnRecv(System.IO.MemoryStream stream, int recvcount)
        {
            var p = stream.Position;
            var msg = NetMessage.Unpack(stream);
            var pend = stream.Position;
            if (pend - p > recvcount)
                throw new Exception("error net message.");

            var iddata = msg.Params["_id"];
            if (iddata.Length != 8)
                throw new Exception("error net message _id");

            switch (msg.Cmd)
            {
                case "_ping":
                    await OnPing(msg, iddata);
                    break;
                case "_db.state":
                    await OnDB_State(msg, iddata);
                    break;

                ///snapshot interface
                case "_db.usesnapshot":
                    await OnDB_UseSnapShot(msg, iddata);
                    break;
                case "_db.unusesnapshot":
                    await OnDB_UnuseSnapShot(msg, iddata);
                    break;
                case "_db.snapshot.dataheight":
                    await OnSnapshotDataHeight(msg, iddata);//ulong DataHeight { get; }
                    break;
                case "_db.snapshot.getvalue":
                    await OnSnapshot_GetValue(msg, iddata);//DBValue GetValue(byte[] tableid, byte[] key);
                    break;
                case "_db.snapshot.gettableinfo":
                    await OnSnapshot_GetTableInfo(msg, iddata);//TableInfo GetTableInfo(byte[] tableid);
                    break;
                case "_db.snapshot.gettablecount":
                    await OnSnapshot_GetTableCount(msg, iddata); //uint GetTableCount(byte[] tableid);
                    break;
                case "_db.snapshot.newiterator":
                    await OnSnapshot_CreateKeyIterator(msg, iddata); //CreateKeyIterator(byte[] tableid, byte[] _beginkey = null, byte[] _endkey = null);
                    break;
                case "_db.iterator.current":
                    await OnSnapshot_IteratorCurrent(msg, iddata); //CreateKeyIterator(byte[] tableid, byte[] _beginkey = null, byte[] _endkey = null);
                    break;
                case "_db.iterator.next":
                    await OnSnapshot_IteratorNext(msg, iddata); //CreateKeyIterator(byte[] tableid, byte[] _beginkey = null, byte[] _endkey = null);
                    break;
                case "_db.iterator.reset":
                    await OnSnapshot_IteratorReset(msg, iddata); //CreateKeyIterator(byte[] tableid, byte[] _beginkey = null, byte[] _endkey = null);
                    break;

                ///write method
                case "_db.write":
                    await OnDB_Write(msg, iddata);
                    break;
                //要扩展几个东西,直接获取block信息和blockhash,获取有写入权限的地址
                case "_db.snapshot.getblock":
                    await OnSnapshotEXT_GetBlock(msg, iddata);
                    break;
                case "_db.snapshot.getblockhash":
                    await OnSnapshotEXT_GetBlockHash(msg, iddata);
                    break;
                case "_db.snapshot.getwriter":
                    await OnSnapshotEXT_GetWriter(msg, iddata);
                    break;
                //还需要扩展，查询的时候，一次返回多个的
                //case "_db.iteratorex.nextmulti":
                default:
                    throw new Exception("unknown msg cmd:" + msg.Cmd);
            }
        }
        public async Task OnPing(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_ping.back");
            msg.Params["_id"] = id;
            SendToClient(msg);

        }
        public async Task OnDB_State(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.state.back");
            msg.Params["_id"] = id;
            msg.Params["dbopen"] = new byte[] { (byte)(Program.storage.state_DBOpen ? 1 : 0) };
            if (Program.storage.state_DBOpen)
            {
                try
                {
                    using (var snap = Program.storage.maindb.UseSnapShot())
                    {
                        msg.Params["height"] = BitConverter.GetBytes(snap.DataHeight);
                        //Console.WriteLine("dataheight=" + snap.DataHeight);
                        var value = snap.GetValue(LightDB.systemtable_info, "_magic_".ToBytes_UTF8Encode());
                        if (value != null)
                            msg.Params["magic"] = value.value;

                        var keys = snap.CreateKeyFinder(StorageService.tableID_Writer);
                        var i = 0;
                        foreach (byte[] keybin in keys)
                        {
                            //var key = keybin.ToString_UTF8Decode();
                            var iswriter = snap.GetValue(StorageService.tableID_Writer, keybin).AsBool();
                            if (iswriter)
                            {
                                i++;
                                msg.Params["writer" + i] = keybin;
                                //Console.WriteLine("writer:" + key);
                            }
                        }
                    }
                }
                catch (Exception err)
                {
                    msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
                }
            }
            else
            {
                msg.Params["_error"] = "db not open.".ToBytes_UTF8Encode();
            }
            SendToClient(msg);

        }

        //收集所有snapshot
        public System.Collections.Concurrent.ConcurrentDictionary<UInt64, ISnapShot> peerSnapshots;
        public System.Collections.Concurrent.ConcurrentDictionary<UInt64, IEnumerator<byte[]>> peerItertors;
        public async Task OnDB_UseSnapShot(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.usesnapshot.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64? wantheight = msgRecv.Params.ContainsKey("snapheight") ? (UInt64?)BitConverter.ToUInt64(msgRecv.Params["snapheight"], 0) : null;
                if (wantheight == null)
                {
                    var snapshot = Program.storage.maindb.UseSnapShot();
                    wantheight = snapshot.DataHeight;
                    peerSnapshots[snapshot.DataHeight] = snapshot;
                }
                else
                {
                    if (peerSnapshots.ContainsKey(wantheight.Value) == false)
                    {
                        msg.Params["_error"] = "do not have that snapheight".ToBytes_UTF8Encode();
                    }
                }
                if (msg.Params.ContainsKey("_error") == false)
                {
                    msg.Params["snapheight"] = BitConverter.GetBytes(wantheight.Value);
                }
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            SendToClient(msg);
        }
        public async Task OnDB_UnuseSnapShot(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.unusesnapshot.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msgRecv.Params["snapheight"]);
                bool b = peerSnapshots.TryRemove(snapheight, out ISnapShot value);
                if (b)
                {
                    value.Dispose();
                }
                msg.Params["remove"] = new byte[] { (byte)(b ? 1 : 0) };
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            SendToClient(msg);
        }
        public async Task OnSnapshotDataHeight(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.dataheight.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msgRecv.Params["snapheight"]);
                var snap = peerSnapshots[snapheight];
                msg.Params["dataheight"] = BitConverter.GetBytes(snap.DataHeight);

            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            SendToClient(msg);
        }
        public async Task OnSnapshot_GetValue(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.getvalue.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msgRecv.Params["snapheight"]);
                var snap = peerSnapshots[snapheight];
                byte[] data = snap.GetValueData(msgRecv.Params["tableid"], msgRecv.Params["key"]);
                msg.Params["data"] = data;
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
        public async Task OnSnapshotEXT_GetBlock(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.getblock.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msgRecv.Params["snapheight"]);
                var snap = peerSnapshots[snapheight];
                byte[] data = snap.GetValueData(LightDB.systemtable_block, msgRecv.Params["blockid"]);
                msg.Params["data"] = data;
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
        public async Task OnSnapshotEXT_GetBlockHash(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.getblockhash.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msgRecv.Params["snapheight"]);
                var snap = peerSnapshots[snapheight];
                byte[] data = snap.GetValueData(StorageService.tableID_BlockID2Hash, msgRecv.Params["blockid"]);
                msg.Params["data"] = data;
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
        public async Task OnSnapshotEXT_GetWriter(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.getwriter.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msgRecv.Params["snapheight"]);
                var snap = peerSnapshots[snapheight];
                var allwriter = snap.CreateKeyFinder(StorageService.tableID_Writer);
                int n = 0;
                foreach (var wkey in allwriter)
                {
                    var v = snap.GetValue(StorageService.tableID_Writer, wkey);
                    if (v.AsBool() == true)
                    {
                        msg.Params["writer" + n] = wkey;
                        n++;
                    }
                }
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
        public async Task OnSnapshot_GetTableCount(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.gettablecount.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msgRecv.Params["snapheight"]);
                var snap = peerSnapshots[snapheight];
                byte[] data = BitConverter.GetBytes(snap.GetTableCount(msgRecv.Params["tableid"]));
                msg.Params["count"] = data;
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
        public async Task OnSnapshot_GetTableInfo(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.gettableinfo.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msgRecv.Params["snapheight"]);
                var snap = peerSnapshots[snapheight];

                //此处可以优化，增加一个GetTableInfoData,不要转一次
                byte[] data = snap.GetTableInfo(msgRecv.Params["tableid"]).ToBytes();
                msg.Params["info"] = data;
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
        public async Task OnSnapshot_CreateKeyIterator(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("__db.snapshot.newiterator.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msgRecv.Params["snapheight"]);
                var snap = peerSnapshots[snapheight];

                //这里缺一个给唯一ID的方式,采用顺序编号是个临时方法
                var beginkey = msgRecv.Params?["beginkey"];
                var endkey = msgRecv.Params?["endkey"];
                var iter = snap.CreateKeyIterator(msgRecv.Params["tableid"], beginkey, endkey);
                ulong index = (UInt64)this.peerItertors.Count;
                this.peerItertors[index] = iter;

                msg.Params["iteratorid"] = BitConverter.GetBytes(index);
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);

        }
        public async Task OnSnapshot_IteratorCurrent(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.iterator.current.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 iteratorid = BitConverter.ToUInt64(msgRecv.Params["iteratorid"]);
                var it = peerItertors[iteratorid];

                var data = it.Current;
                msg.Params["data"] = data;
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
        public async Task OnSnapshot_IteratorNext(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.iterator.next.back");
            msg.Params["_id"] = id;

            try
            {
                UInt64 iteratorid = BitConverter.ToUInt64(msgRecv.Params["iteratorid"]);
                var it = peerItertors[iteratorid];

                var b = it.MoveNext();
                msg.Params["movenext"] = new byte[] { (byte)(b ? 1 : 0) };
                var data = it.Current;
                msg.Params["data"] = data;
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
        public async Task OnSnapshot_IteratorReset(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.iterator.reset.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 iteratorid = BitConverter.ToUInt64(msgRecv.Params["iteratorid"]);
                var it = peerItertors[iteratorid];

                it.Reset();
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
