using CafeApp.Commands;
using CafeApp.Domain;
using CafeApp.Events;
using Edument.CQRS;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TestStack.BDDfy;
using Xunit;

namespace CafeApp.Tests
{
    [Story(AsA="", IWant="", SoThat="")]
    public class TabTests
    {
        private Guid testId;
        private int testTable;
        private string testWaiter;
        private TabAggregate sut;

        [Fact]
        public void CanOpenANewTab()
        {
            sut = new TabAggregate();
            testId = Guid.NewGuid();
            testTable = 42;
            testWaiter = "Derek";
            var command = new OpenTab()
                {
                    Id = testId,
                    TableNumber = testTable,
                    Waiter = testWaiter
                };
            var expectedEvent = new TabOpened()
                {
                    Id = testId,
                    TableNumber = testTable,
                    Waiter = testWaiter
                };
            IEnumerable<TabOpened> actualEvent = null;
            this.When(_=> WhenIRunCommand(command, out actualEvent))
                .Then(_=> ThenTheTabOpenedEventIsProduced(expectedEvent, actualEvent))
                .BDDfy();
        }

        private void WhenIRunCommand(OpenTab command, out IEnumerable<TabOpened> actualEvents)
        {
            var commandHandler = sut as IHandleCommand<OpenTab>;
            var actual = commandHandler.Handle(command);
            actualEvents = actual.Cast<TabOpened>();// as IEnumerable<TabOpened>;
            Assert.NotNull(actualEvents);
        }

        private void ThenTheTabOpenedEventIsProduced(TabOpened expectedEvent, IEnumerable<TabOpened> actualEvents)
        {
            Assert.Equal(1, actualEvents.Count());
            var actual = actualEvents.First();
            Assert.Equal(expectedEvent.Id, actual.Id);
            Assert.Equal(expectedEvent.TableNumber, actual.TableNumber);
            Assert.Equal(expectedEvent.Waiter, actual.Waiter);
        }
    }
}
