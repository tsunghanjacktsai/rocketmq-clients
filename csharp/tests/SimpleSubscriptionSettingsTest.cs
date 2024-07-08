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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Apache.Rocketmq.V2;
using Castle.Core.Internal;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;
using Endpoints = Org.Apache.Rocketmq.Endpoints;
using FilterExpression = Org.Apache.Rocketmq.FilterExpression;

namespace tests
{
    [TestClass]
    public class SimpleSubscriptionSettingsTest
    {
        [TestMethod]
        public void TestToProtobuf()
        {
            var groupResource = "testConsumerGroup";
            var clientId = "testClientId";
            var subscriptionExpression = new ConcurrentDictionary<string, FilterExpression>(
                new Dictionary<string, FilterExpression> {{"testTopic", new FilterExpression("*")}});
            var requestTimeout = TimeSpan.FromSeconds(3);
            var longPollingTimeout = TimeSpan.FromSeconds(15);
            var simpleSubscriptionSettings = new SimpleSubscriptionSettings(
                "testNamespace",
                clientId,
                new Endpoints("127.0.0.1:9876"),
                groupResource,
                requestTimeout,
                longPollingTimeout,
                subscriptionExpression
            );
            var settings = simpleSubscriptionSettings.ToProtobuf();

            Assert.AreEqual(Proto.ClientType.SimpleConsumer, settings.ClientType);
            Assert.AreEqual(Duration.FromTimeSpan(requestTimeout), settings.RequestTimeout);
            Assert.IsFalse(settings.Subscription.Subscriptions.IsNullOrEmpty());
            var subscription = settings.Subscription;
            Assert.AreEqual(subscription.Group, new Proto.Resource
            {
                ResourceNamespace = "testNamespace",
                Name = "testConsumerGroup"
            });
            Assert.IsFalse(subscription.Fifo);
            Assert.AreEqual(Duration.FromTimeSpan(longPollingTimeout), subscription.LongPollingTimeout);
            var subscriptionsList = subscription.Subscriptions;
            Assert.AreEqual(1, subscriptionsList.Count);
            var subscriptionEntry = subscriptionsList[0];
            Assert.AreEqual(FilterType.Tag, subscriptionEntry.Expression.Type);
            Assert.AreEqual(subscriptionEntry.Topic, new Proto.Resource
            {
                ResourceNamespace = "testNamespace",
                Name = "testTopic"
            });
        }

        [TestMethod]
        public void TestSync()
        {
            var groupResource = "testConsumerGroup";
            var clientId = "testClientId";
            var subscriptionExpression = new ConcurrentDictionary<string, FilterExpression>(
                new Dictionary<string, FilterExpression> {{"testTopic", new FilterExpression("*")}});
            var requestTimeout = TimeSpan.FromSeconds(3);
            var longPollingTimeout = TimeSpan.FromSeconds(15);
            var simpleSubscriptionSettings = new SimpleSubscriptionSettings(
                "testNamespace",
                clientId,
                new Endpoints("127.0.0.1:9876"),
                groupResource,
                requestTimeout,
                longPollingTimeout,
                subscriptionExpression
            );
            var subscription = new Proto.Subscription
            {
                Fifo = true
            };
            var settings = new Proto.Settings
            {
                Subscription = subscription
            };
            simpleSubscriptionSettings.Sync(settings);
        }
    }
}