# 🚀 E-Commerce User Behavior Analysis System

![Java](https://img.shields.io/badge/Java-11-orange)
![Flink](https://img.shields.io/badge/Apache%20Flink-1.12-blue)
![Architecture](https://img.shields.io/badge/Architecture-Modular-green)

A production-ready data processing engine built with **Apache Flink**, designed to handle high-throughput e-commerce user logs. This project implements a **Hybrid Architecture** that performs **Real-time Risk Control** (Anti-Scalping) and **Trend Analysis** (Hot Items) simultaneously.

## 🌟 Key Highlights

* **Modular Design**: Refactored into `Model`, `Operators`, and `Jobs` layers for maximum code reusability and maintainability.
* **Hybrid Processing**: Combines low-latency alerting (milliseconds) with daily reporting (batch-like) in a single Flink job.
* **Advanced Flink Patterns**:
    * **Side Outputs**: Isolate anomaly data (Risk Control) from the main stream without blocking processing.
    * **State Management**: Efficient use of `KeyedState` to track user frequency without external databases.
    * **Timers**: Event-time timers for precise daily window triggering.

## 🛠 Project Architecture

The project follows a clean **Separation of Concerns** principle (MVC-like structure):

```text
src/main/java/com/zoeliu/flink/
├── model/                     # [Data Layer] Pure POJOs
│   ├── UserBehavior.java      # Normalized user event schema
│   ├── ItemViewCount.java     # Aggregation result schema
│   └── UrlViewCount.java      # Web traffic schema (Extensible)
│
├── operators/                 # [Logic Layer] Reusable Flink UDFs
│   └── TopNWithOnTimer.java  # Generic Top-N sorting operator (ProcessFunction)
│
└── jobs/                      # [Application Layer] Business Entry Points
    ├── HybridUserAnalysis.java  # [Core] Risk Control & Daily Reporting 
    ├── HotItemsAnalysis.java    # [Core] Real-time Trending Products
    └── ActiveUserAnalysis.java  # Hourly UV Statistics
```
## 🧠 Core Features & Implementation
1. **Real-time Risk Control (Anti-Scalping)**
    * Goal: Detect malicious users or bots refreshing pages too frequently.

    * Logic: Tracks user clicks in ValueState. If a user exceeds the threshold (e.g., 100 clicks) within a defined period, they are immediately flagged.

    * Tech: KeyedProcessFunction, SideOutput.

2. **Real-time Hot Items (Trending)**
    * Goal: Calculate the "Top N" most viewed items every 5 minutes.

    * Logic: Sliding Window (1hr length, 5min slide) -> Aggregate -> Sort.

    * Tech: SlidingEventTimeWindows, Custom TopNItemsProcess operator.

3. **Daily Activity Report**
    * Goal: Automatically generate a statistical report at 00:00 every day.

    * Tech: TimerService registered on Event Time to trigger state flushing.

## 💻 Getting Started

### Prerequisites
* Java 8 or 11
* Maven 3.x
* IntelliJ IDEA (Recommended)

### How to Run

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/zoeee06/flink-user-behavior-analysis.git
    ```

2.  **Verify Data Source**:
    Ensure the dataset exists at `src/main/resources/UserBehavior.csv`.

3.  **Execute Jobs**:
    Run specific jobs based on your analysis needs (see below).


## Job Descriptions & Sample Outputs
1. **Risk Control (HybridUserAnalysis.java)**
    * Goal: Detect malicious users (bots/scalpers). 
    * Logic: Monitors user click frequency. If a user hits the specific threshold (e.g., 20 clicks), an alert is triggered immediately. 
    * Console Output:
   ```text
    🚨 ALERT> [RISK DETECTED] Time:1511664497000 | User:258029 | Action: Threshold Exceeded (20)
    🚨 ALERT> [RISK DETECTED] Time:1511667787000 | User:105835 | Action: Threshold Exceeded (20)
    🚨 ALERT> [RISK DETECTED] Time:1511669929000 | User:973806 | Action: Threshold Exceeded (20)
    ```
   2. **Hot Items Analysis (HotItemsAnalysis.java)**
   * Goal: Identify the Top 5 most viewed items over the last hour, updated every 5 minutes. 
   * Logic: Sliding Window -> Aggregate -> Sort (using custom TopNItemsProcess operator). 
   * Console Output:
   ```text
   ========================================
    Window End: 2017-11-25 20:05:00.0
    No.1:  ItemID=5051027  Views=3
    No.2:  ItemID=3493253  Views=3
    No.3:  ItemID=4261030  Views=3
    No.4:  ItemID=4894670  Views=2
    No.5:  ItemID=3781391  Views=2
    ========================================

    ========================================
    Window End: 2017-11-25 20:10:00.0
    No.1:  ItemID=812879  Views=5
    No.2:  ItemID=2600165  Views=4
    No.3:  ItemID=2828948  Views=4
    No.4:  ItemID=2338453  Views=4
    No.5:  ItemID=4261030  Views=4
    ========================================
    ```
   3. **Active User Statistics (ActiveUserAnalysis.java)**
    * Goal: Count unique visitors (UV) per hour. 
    * Logic: Uses TumblingEventTimeWindows with deduplication logic. 
    * Console Output:
   ```text
   Window: 1511658000000 ~ 1511661600000 | Active Users: 28196
   Window: 1511661600000 ~ 1511665200000 | Active Users: 32160
   Window: 1511665200000 ~ 1511668800000 | Active Users: 32233
   Window: 1511668800000 ~ 1511672400000 | Active Users: 30615
   Window: 1511672400000 ~ 1511676000000 | Active Users: 32747
   Window: 1511676000000 ~ 1511679600000 | Active Users: 33898
   Window: 1511679600000 ~ 1511683200000 | Active Users: 34631
   Window: 1511683200000 ~ 1511686800000 | Active Users: 34746
   Window: 1511686800000 ~ 1511690400000 | Active Users: 32356
   Window: 1511690400000 ~ 1511694000000 | Active Users: 13
    ```

## 📝 Future Improvements
* Integration with **Kafka** for production data ingestion.
* Replace file-based State with **Redis** for distributed state sharing.
* Dashboard visualization using **Grafana**.