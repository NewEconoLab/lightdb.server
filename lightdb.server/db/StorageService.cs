using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using LightDB;
namespace LightDB.Server
{

    public class StorageService
    {
        public static readonly byte[] tableID_Writer = new byte[] { 0x07 };
        public static readonly byte[] tableID_BlockID2Hash = new byte[] { 0x08 };
        public static readonly byte[] tableID_BlockID2Verifiy = new byte[] { 0x09 };

        public LightDB maindb;
        public bool state_DBOpen
        {
            get;
            private set;
        }

        public void Init()
        {
            Console.CursorLeft = 0;
            Console.WriteLine(" == Open DB ==");


            maindb = new LightDB();

            state_DBOpen = false;
            string fullpath = System.IO.Path.GetFullPath(Program.config.server_storage_path);
            if (System.IO.Directory.Exists(fullpath) == false)
                System.IO.Directory.CreateDirectory(fullpath);
            string pathDB = System.IO.Path.Combine(fullpath, "maindb");
            try
            {
                maindb.Open(pathDB);
                state_DBOpen = true;
                Console.WriteLine("db opened in:" + pathDB);
            }
            catch (Exception err)
            {
                Console.WriteLine("error msg:" + err.Message);
            }
            if (state_DBOpen == false)
            {
                Console.WriteLine("open database fail. try to create it.");
                try
                {
                    DBCreateOption createop = new DBCreateOption();
                    createop.MagicStr = Program.config.storage_maindb_magic;
                    createop.FirstTask = new WriteTask();

                    createop.FirstTask.CreateTable(new TableInfo(tableID_Writer, "_writeraddress_", "", DBValue.Type.String));

                    createop.FirstTask.Put(tableID_Writer, Program.config.storage_maindb_firstwriter_address.ToBytes_UTF8Encode(), DBValue.FromValue(DBValue.Type.BOOL, true));

                    createop.FirstTask.CreateTable(new TableInfo(tableID_BlockID2Hash, "_block:index->hash_", "", DBValue.Type.String));
                    createop.FirstTask.CreateTable(new TableInfo(tableID_BlockID2Verifiy, "_block:index->hash_", "", DBValue.Type.String));

                    var srcdata = createop.FirstTask.ToBytes();
                    var hash = Helper.Sha256.ComputeHash(srcdata);

                    //用后处理写入hash
                    createop.afterparser = (_task, _data, _wb) =>
                      {
                          var keyindexzero = new byte[8];

                          //填充创世块hash
                          _wb.Put(StorageService.tableID_BlockID2Hash, keyindexzero, DBValue.FromValue(DBValue.Type.Bytes, hash));

                          //填充一个空字，校验数据
                          _wb.Put(StorageService.tableID_BlockID2Verifiy, keyindexzero, DBValue.FromValue(DBValue.Type.Bytes, new byte[1]));
                      };

                    maindb.Open(pathDB, createop);
                    Console.WriteLine("db created in:" + pathDB);
                    state_DBOpen = true;
                }
                catch (Exception err)
                {
                    Console.WriteLine("error msg:" + err.Message);

                }
            }
        }

    }
}
