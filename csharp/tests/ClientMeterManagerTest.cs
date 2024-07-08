/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Collections.Concurrent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Metric = Org.Apache.Rocketmq.Metric;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class ClientMeterManagerTest
    {
        private const string FakeEndpoint = "127.0.0.1:8080";

        [TestMethod]
        public void TestResetWithMetricOn()
        {
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints(FakeEndpoint)
                .Build();
            var meterManager = new ClientMeterManager(CreateTestClient());
            var endpoints0 = new Proto.Endpoints { Scheme = Proto.AddressScheme.Ipv4,
                Addresses = { new Proto.Address { Host = "127.0.0.1", Port = 8080 }}};
            var metric = new Metric(new Proto.Metric { On = true, Endpoints = endpoints0 });
            meterManager.Reset(metric);
            Assert.IsTrue(meterManager.IsEnabled());
        }

        [TestMethod]
        public void TestResetWithMetricOff()
        {
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints(FakeEndpoint)
                .Build();
            var meterManager = new ClientMeterManager(CreateTestClient());
            var endpoints0 = new Proto.Endpoints { Scheme = Proto.AddressScheme.Ipv4,
                Addresses = { new Proto.Address { Host = "127.0.0.1", Port = 8080 }}};
            var metric = new Metric(new Proto.Metric { On = false, Endpoints = endpoints0 });
            meterManager.Reset(metric);
            Assert.IsFalse(meterManager.IsEnabled());
        }
        
        private Client CreateTestClient()
        {
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints(FakeEndpoint)
                .Build();
            return new PushConsumer(clientConfig, "testGroup",
                new ConcurrentDictionary<string, FilterExpression>(), new TestMessageListener(),
                0, 0, 1);
        }
        
        private class TestMessageListener : IMessageListener
        {
            public ConsumeResult Consume(MessageView messageView)
            {
                return ConsumeResult.SUCCESS;
            }
        }
    }
}