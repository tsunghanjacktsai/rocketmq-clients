using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class PushConsumerTest
    {
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task TestSubscribeBeforeStartup()
        {
            var pushConsumer = CreatePushConsumer();
            await pushConsumer.Subscribe("testTopic", new FilterExpression("*"));
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestUnsubscribeBeforeStartup()
        {
            var pushConsumer = CreatePushConsumer();
            pushConsumer.Unsubscribe("testTopic");
        }
        
        [TestMethod]
        public async Task TestQueryAssignment()
        {
            var pushConsumer = CreatePushConsumer();
            var metadata = pushConsumer.Sign();
            var mq = new Proto.MessageQueue
            {
                Broker = new Proto.Broker
                {
                    Name = "broker0",
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses =
                        {
                            new Proto.Address
                            {
                                Host = "127.0.0.1",
                                Port = 8080
                            }
                        }
                    }
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "testNamespace",
                    Name = "testTopic",
                },
                AcceptMessageTypes = { Proto.MessageType.Normal }
            };
            var queryRouteResponse = new Proto.QueryRouteResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                MessageQueues = { mq }
            };
            var queryRouteInvocation = new RpcInvocation<Proto.QueryRouteRequest, Proto.QueryRouteResponse>(null,
                queryRouteResponse, metadata);
            var mockClientManager = new Mock<IClientManager>();
            mockClientManager.Setup(cm =>
                cm.QueryRoute(It.IsAny<Endpoints>(), It.IsAny<Proto.QueryRouteRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(queryRouteInvocation));
            var mockCall = new AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand>(
                new MockClientStreamWriter<Proto.TelemetryCommand>(),
                new MockAsyncStreamReader<Proto.TelemetryCommand>(),
                null,
                null,
                null,
                null);
            var queryAssignmentResponse = new Proto.QueryAssignmentResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                Assignments = {  }
            };
            var queryAssignmentInvocation =
                new RpcInvocation<Proto.QueryAssignmentRequest, Proto.QueryAssignmentResponse>(null,
                    queryAssignmentResponse, metadata);
            mockClientManager.Setup(cm =>
                    cm.QueryAssignment(It.IsAny<Endpoints>(), It.IsAny<Proto.QueryAssignmentRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(queryAssignmentInvocation));
            mockClientManager.Setup(cm => cm.Telemetry(It.IsAny<Endpoints>())).Returns(mockCall);
            pushConsumer.SetClientManager(mockClientManager.Object);
            await pushConsumer.QueryAssignment("testTopic");
        }
        
        [TestMethod]
        public async Task TestScanAssignments()
        {
            var pushConsumer = CreatePushConsumer();
            var metadata = pushConsumer.Sign();
            var mq = new Proto.MessageQueue
            {
                Broker = new Proto.Broker
                {
                    Name = "broker0",
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses =
                        {
                            new Proto.Address
                            {
                                Host = "127.0.0.1",
                                Port = 8080
                            }
                        }
                    }
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "testNamespace",
                    Name = "testTopic",
                },
                AcceptMessageTypes = { Proto.MessageType.Normal }
            };
            var queryRouteResponse = new Proto.QueryRouteResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                MessageQueues = { mq }
            };
            var queryRouteInvocation = new RpcInvocation<Proto.QueryRouteRequest, Proto.QueryRouteResponse>(null,
                queryRouteResponse, metadata);
            var mockClientManager = new Mock<IClientManager>();
            mockClientManager.Setup(cm =>
                cm.QueryRoute(It.IsAny<Endpoints>(), It.IsAny<Proto.QueryRouteRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(queryRouteInvocation));
            var mockCall = new AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand>(
                new MockClientStreamWriter<Proto.TelemetryCommand>(),
                new MockAsyncStreamReader<Proto.TelemetryCommand>(),
                null,
                null,
                null,
                null);
            var queryAssignmentResponse = new Proto.QueryAssignmentResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                Assignments = { new Proto.Assignment
                {
                    MessageQueue = mq
                } }
            };
            var queryAssignmentInvocation =
                new RpcInvocation<Proto.QueryAssignmentRequest, Proto.QueryAssignmentResponse>(null,
                    queryAssignmentResponse, metadata);
            mockClientManager.Setup(cm =>
                    cm.QueryAssignment(It.IsAny<Endpoints>(), It.IsAny<Proto.QueryAssignmentRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(queryAssignmentInvocation));
            mockClientManager.Setup(cm => cm.Telemetry(It.IsAny<Endpoints>())).Returns(mockCall);
            pushConsumer.SetClientManager(mockClientManager.Object);
            pushConsumer.State = State.Running;
            await pushConsumer.Subscribe("testTopic", new FilterExpression("*"));
            pushConsumer.ScanAssignments();
        }
        
        [TestMethod]
        public async Task TestScanAssignmentsWithoutResults()
        {
            var pushConsumer = CreatePushConsumer();
            var metadata = pushConsumer.Sign();
            var mq = new Proto.MessageQueue
            {
                Broker = new Proto.Broker
                {
                    Name = "broker0",
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses =
                        {
                            new Proto.Address
                            {
                                Host = "127.0.0.1",
                                Port = 8080
                            }
                        }
                    }
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "testNamespace",
                    Name = "testTopic",
                },
                AcceptMessageTypes = { Proto.MessageType.Normal }
            };
            var queryRouteResponse = new Proto.QueryRouteResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                MessageQueues = { mq }
            };
            var queryRouteInvocation = new RpcInvocation<Proto.QueryRouteRequest, Proto.QueryRouteResponse>(null,
                queryRouteResponse, metadata);
            var mockClientManager = new Mock<IClientManager>();
            mockClientManager.Setup(cm =>
                cm.QueryRoute(It.IsAny<Endpoints>(), It.IsAny<Proto.QueryRouteRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(queryRouteInvocation));
            var mockCall = new AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand>(
                new MockClientStreamWriter<Proto.TelemetryCommand>(),
                new MockAsyncStreamReader<Proto.TelemetryCommand>(),
                null,
                null,
                null,
                null);
            var queryAssignmentResponse = new Proto.QueryAssignmentResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                Assignments = { }
            };
            var queryAssignmentInvocation =
                new RpcInvocation<Proto.QueryAssignmentRequest, Proto.QueryAssignmentResponse>(null,
                    queryAssignmentResponse, metadata);
            mockClientManager.Setup(cm =>
                    cm.QueryAssignment(It.IsAny<Endpoints>(), It.IsAny<Proto.QueryAssignmentRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(queryAssignmentInvocation));
            mockClientManager.Setup(cm => cm.Telemetry(It.IsAny<Endpoints>())).Returns(mockCall);
            pushConsumer.SetClientManager(mockClientManager.Object);
            pushConsumer.State = State.Running;
            await pushConsumer.Subscribe("testTopic", new FilterExpression("*"));
            pushConsumer.ScanAssignments();
        }
        
        private PushConsumer CreatePushConsumer()
        {
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints("127.0.0.1")
                .Build();
            return new PushConsumer(clientConfig, "testGroup",
                new ConcurrentDictionary<string, FilterExpression>(), new TestMessageListener(),
                10, 10, 1);
        }
        
        private class TestMessageListener : IMessageListener
        {
            public ConsumeResult Consume(MessageView messageView)
            {
                return ConsumeResult.SUCCESS;
            }
        }
        
        private class MockClientStreamWriter<T> : IClientStreamWriter<T>
        {
            public Task WriteAsync(T message)
            {
                // Simulate async operation
                return Task.CompletedTask;
            }

            public WriteOptions WriteOptions { get; set; }
            
            public Task CompleteAsync()
            {
                throw new NotImplementedException();
            }
        }
        
        private class MockAsyncStreamReader<T> : IAsyncStreamReader<T>
        {
            public Task<bool> MoveNext(CancellationToken cancellationToken)
            {
                throw new System.NotImplementedException();
            }

            public T Current => throw new NotImplementedException();
        }
    }
}