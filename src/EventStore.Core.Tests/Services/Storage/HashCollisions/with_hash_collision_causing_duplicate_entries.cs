using System;
using EventStore.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using NUnit.Framework;

namespace EventStore.Core.Tests.ClientAPI
{
    [TestFixture]
    public class with_hash_collision_causing_duplicate_entries : SpecificationWithDirectoryPerTestFixture
    {
        private MiniNode _node;

        protected virtual IEventStoreConnection BuildConnection(MiniNode node)
        {
            return TestConnection.To(node, TcpType.Normal);
        }

        [TestFixtureSetUp]
        public override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            _node = new MiniNode(PathName,
            	memTableSize: 50,
            	hashCollisionReadLimit: 1,
            	indexBitnessVersion: EventStore.Core.Index.PTableVersions.Index32Bit);
            _node.Start();
        }

        [TestFixtureTearDown]
        public override void TestFixtureTearDown()
        {
            _node.Shutdown();
            base.TestFixtureTearDown();
        }

        [Test, Category("duplicate")]
        public void should_not_cause_duplicate_entries()
        {
            const string stream1 = "LPN-FC002_LPK51001";
            const string stream2 = "account--696193173";
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                //Write event to stream 1
	            Assert.AreEqual(0, store.AppendToStreamAsync(stream1, ExpectedVersion.EmptyStream, new EventData(Guid.NewGuid(), "TestEvent", true, null, null)).Result.NextExpectedVersion);
	            //Write 10 events to stream 2 which will have the same hash as stream 1.
	            for(int i = 0; i < 10; i++){
	                Assert.AreEqual(i, store.AppendToStreamAsync(stream2, ExpectedVersion.Any, new EventData(Guid.NewGuid(), "TestEvent", true, null, null)).Result.NextExpectedVersion);
	            }
            }
            //Restart the node to ensure the read index stream info cache is empty
            _node.Shutdown(keepDb: true, keepPorts: true);
            _node = new MiniNode(PathName,
            	memTableSize: 50,
            	hashCollisionReadLimit: 1,
            	indexBitnessVersion: EventStore.Core.Index.PTableVersions.Index32Bit);
           _node.Start();
            using (var store = BuildConnection(_node))
            {
                store.ConnectAsync().Wait();
                //Verify the hash collision has resulted in a read returning no stream
                Assert.AreEqual(SliceReadStatus.StreamNotFound, store.ReadStreamEventsForwardAsync(stream1, 0, 2, resolveLinkTos: false).Result.Status);
                //The following should return 1
	            Assert.AreEqual(1, store.AppendToStreamAsync(stream1, ExpectedVersion.Any, new EventData(Guid.NewGuid(), "TestEvent", true, null, null)).Result.NextExpectedVersion);
            }
        }
    }
}
