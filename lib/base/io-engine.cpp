/* Icinga 2 | (c) 2012 Icinga GmbH | GPLv2+ */

#include "base/configuration.hpp"
#include "base/exception.hpp"
#include "base/io-engine.hpp"
#include "base/lazy-init.hpp"
#include "base/logger.hpp"
#include <exception>
#include <memory>
#include <thread>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/system/error_code.hpp>

using namespace icinga;

CpuBoundWork::CpuBoundWork(boost::asio::yield_context yc, boost::asio::io_context::strand& strand)
	: m_Done(false)
{
	VERIFY(strand.running_in_this_thread());

	auto& ie (IoEngine::Get());
	Shared<AsioConditionVariable>::Ptr cv;

	auto fastPath = [&ie] {
		auto freeSlots (ie.m_CpuBoundSemaphore.load());

		while (freeSlots > 0) {
			// A failure here is equivalent to loading the latest value into freeSlots
			if (ie.m_CpuBoundSemaphore.compare_exchange_weak(freeSlots, freeSlots - 1)) {
				return true;
			}
		}

		return false;
	};

	while (!fastPath()) {
		if (!cv) {
			cv = Shared<AsioConditionVariable>::Make(ie.GetIoContext());

			// The above line may take a little bit, so let's optimistically re-check
			if (fastPath()) {
				break;
			}
		}

		{
			std::unique_lock lock (ie.m_CpuBoundWaitingMutex);

			// The above line may take even longer, so let's check again.
			// Also mitigate lost wake-ups by synchronizing with Done().
			// On details, see Done() implementation inline comments.
			if (fastPath()) {
				break;
			}

			ie.m_CpuBoundWaiting.emplace_back(strand, cv);
		}

		cv->Wait(yc);
	}
}

void CpuBoundWork::Done()
{
	if (!m_Done) {
		m_Done = true;

		auto& ie (IoEngine::Get());

		// The constructor takes the slow path only if the semaphore is full,
		// so we only have to wake up constructors if the semaphore was full.
		// This works because after fetch_add(), fast paths in the constructor will succeed.
		if (ie.m_CpuBoundSemaphore.fetch_add(1) < 1) {
			// So now there are only slow path subscribers from just before the fetch_add() to be woken up.
			// Precisely, only subscribers from just before the fetch_add() which turned 0 to 1.

			decltype(ie.m_CpuBoundWaiting) subscribers;

			{
				// Due to the mutex, wake-ups obviously may happen only before or after constructor subscriptions.
				// Ideally, this wake-up happens after any subscription (obviously).
				std::unique_lock lock (ie.m_CpuBoundWaitingMutex);
				std::swap(subscribers, ie.m_CpuBoundWaiting);
			}
			// But even if it happens before (and gets lost), it's not a problem because now the constructor
			// will lock the mutex and re-check the semaphore which is already >0 (fast path) due to fetch_add().

			for (auto& [strand, cv] : subscribers) {
				boost::asio::post(strand, [cv = std::move(cv)] { cv->NotifyOne(); });
			}
		}
	}
}

LazyInit<std::unique_ptr<IoEngine>> IoEngine::m_Instance ([]() { return std::unique_ptr<IoEngine>(new IoEngine()); });

IoEngine& IoEngine::Get()
{
	return *m_Instance.Get();
}

boost::asio::io_context& IoEngine::GetIoContext()
{
	return m_IoContext;
}

IoEngine::IoEngine() : m_IoContext(), m_KeepAlive(boost::asio::make_work_guard(m_IoContext)), m_Threads(decltype(m_Threads)::size_type(Configuration::Concurrency * 2u)), m_AlreadyExpiredTimer(m_IoContext)
{
	m_AlreadyExpiredTimer.expires_at(boost::posix_time::neg_infin);
	m_CpuBoundSemaphore.store(Configuration::Concurrency * 3u / 2u);

	for (auto& thread : m_Threads) {
		thread = std::thread(&IoEngine::RunEventLoop, this);
	}
}

IoEngine::~IoEngine()
{
	for (auto& thread : m_Threads) {
		boost::asio::post(m_IoContext, []() {
			throw TerminateIoThread();
		});
	}

	for (auto& thread : m_Threads) {
		thread.join();
	}
}

void IoEngine::RunEventLoop()
{
	for (;;) {
		try {
			m_IoContext.run();

			break;
		} catch (const TerminateIoThread&) {
			break;
		} catch (const std::exception& e) {
			Log(LogCritical, "IoEngine", "Exception during I/O operation!");
			Log(LogDebug, "IoEngine") << "Exception during I/O operation: " << DiagnosticInformation(e);
		}
	}
}

AsioEvent::AsioEvent(boost::asio::io_context& io, bool init)
	: m_Timer(io)
{
	m_Timer.expires_at(init ? boost::posix_time::neg_infin : boost::posix_time::pos_infin);
}

void AsioEvent::Set()
{
	m_Timer.expires_at(boost::posix_time::neg_infin);
}

void AsioEvent::Clear()
{
	m_Timer.expires_at(boost::posix_time::pos_infin);
}

void AsioEvent::Wait(boost::asio::yield_context yc)
{
	boost::system::error_code ec;
	m_Timer.async_wait(yc[ec]);
}

AsioConditionVariable::AsioConditionVariable(boost::asio::io_context& io)
	: m_Timer(io)
{
	m_Timer.expires_at(boost::posix_time::pos_infin);
}

void AsioConditionVariable::Wait(boost::asio::yield_context yc)
{
	boost::system::error_code ec;
	m_Timer.async_wait(yc[ec]);
}

bool AsioConditionVariable::NotifyOne()
{
	boost::system::error_code ec;
	return m_Timer.cancel_one(ec);
}

size_t AsioConditionVariable::NotifyAll()
{
	boost::system::error_code ec;
	return m_Timer.cancel(ec);
}

/**
 * Cancels any pending timeout callback.
 *
 * Must be called in the strand in which the callback was scheduled!
 */
void Timeout::Cancel()
{
	m_Cancelled->store(true);

	boost::system::error_code ec;
	m_Timer.cancel(ec);
}
