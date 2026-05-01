# 📦 Kafka-Style Distributed Messaging – Full System Design & Implementation

This repository contains the complete system design and backend implementation of a **Kafka-style distributed messaging platform** — built from scratch to mirror the internals of Apache Kafka while remaining approachable and Python-based.

Designed for production realism and educational clarity, this system explores **topics, partitions, consumer groups, leaders, replication, and offset tracking**, gradually evolving from a simple log-based broker to a full distributed messaging cluster with **ZooKeeper-style coordination**.

<div align="center">
    <img src="./final_design.png" alt="System Architecture Diagram" width="2000"/>
</div>

---

## 📘 Course Structure

<table>
  <tbody>
    <!-- CHAPTER 1 -->
    <tr>
      <td colspan="2" align="center"><b>Chapter 1 – <i>Real Kafka Foundations</i></b></td>
    </tr>
    <tr>
      <td><b>1a</b></td>
      <td><a href="chapter_1/section_1a/">E-Commerce App Demo with Real Kafka</a></td>
    </tr>
    <tr>
      <td><b>1b</b></td>
      <td><a href="chapter_1/section_1b/">Setting Up the MySQL Database with Terraform</a></td>
    </tr>
    <tr>
      <td><b>1c</b></td>
      <td><a href="chapter_1/section_1c/">Spinning Up a Local Kafka Cluster with Docker Compose</a></td>
    </tr>
    <tr>
      <td><b>1d</b></td>
      <td><a href="chapter_1/section_1d/">Building the Order Service (Kafka Producer)</a></td>
    </tr>
    <tr>
      <td><b>1e</b></td>
      <td><a href="chapter_1/section_1e/">Building the Payment Service (Kafka Consumer & Producer)</a></td>
    </tr>
    <tr>
      <td><b>1f</b></td>
      <td><a href="chapter_1/section_1f/">Building the Notification Service (Kafka Consumer)</a></td>
    </tr>
    <tr>
      <td><b>1g</b></td>
      <td><a href="chapter_1/section_1g/">Full-Stack AWS Deployment with Terraform & Docker</a></td>
    </tr>
    <!-- CHAPTER 2 -->
    <tr>
      <td colspan="2" align="center"><b>Chapter 2 – <i>Kafkaesque Basics</i></b></td>
    </tr>
    <tr>
      <td><b>2a</b></td>
      <td><a href="chapter_2/section_2a/">Initial Kafkaesque Implementation</a></td>
    </tr>
    <tr>
      <td><b>2b</b></td>
      <td><a href="chapter_2/section_2b/">Kafkaesque Producer API & E-Commerce App Migration</a></td>
    </tr>
    <tr>
      <td><b>2c</b></td>
      <td><a href="chapter_2/section_2c/">Kafkaesque Consumer API</a></td>
    </tr>
    <!-- CHAPTER 3 -->
    <tr>
      <td colspan="2" align="center"><b>Chapter 3 – <i>Scaling Kafkaesque</i></b></td>
    </tr>
    <tr>
      <td><b>3a</b></td>
      <td><a href="chapter_3/section_3a/">Increase Partitions Per Topic to 2</a></td>
    </tr>
    <tr>
      <td><b>3b</b></td>
      <td><a href="chapter_3/section_3b/">Increase Consumers Per Group to 2</a></td>
    </tr>
    <tr>
      <td><b>3c</b></td>
      <td><a href="chapter_3/section_3c/">Increase Brokers to 2</a></td>
    </tr>
    <tr>
      <td><b>3d</b></td>
      <td><a href="chapter_3/section_3d/">Increase Replication Factor to 2</a></td>
    </tr>
    <tr>
      <td><b>3e</b></td>
      <td><a href="chapter_3/section_3e/">In Sync Replicas (ISR) + High Watermarks (HW)</a></td>
    </tr>
    <!-- CHAPTER 4 -->
    <tr>
      <td colspan="2" align="center"><b>Chapter 4 – <i>Kafkaesque ZooKeeper Integration</i></b></td>
    </tr>
    <tr>
      <td><b>4a</b></td>
      <td><a href="chapter_4/section_4a/">ZooKeeper Introduction + Controller Election</a></td>
    </tr>
    <tr>
      <td><b>4b</b></td>
      <td><a href="chapter_4/section_4b/">Controller Znode + Controller Watch</a></td>
    </tr>
    <tr>
      <td><b>4c</b></td>
      <td><a href="chapter_4/section_4c/">Persist Topic Registry in ZK + Topics Watch</a></td>
    </tr>
    <tr>
      <td><b>4d</b></td>
      <td><a href="chapter_4/section_4d/">Partition Assignments Cache, Znode & Watch</a></td>
    </tr>
    <tr>
      <td><b>4e</b></td>
      <td><a href="chapter_4/section_4e/">Peer Broker View + Bootstrap-Aware Replication</a></td>
    </tr>
    <tr>
      <td><b>4f</b></td>
      <td><a href="chapter_4/section_4f/">Request Proxies</a></td>
    </tr>
    <!-- CHAPTER 5 -->
    <tr>
      <td colspan="2" align="center"><b>Chapter 5 – <i>Kafkaesque Packaging & Deployment</i></b></td>
    </tr>
    <tr>
      <td><b>5a</b></td>
      <td><a href="chapter_5/section_5a/">Upload Kafkaesque to PyPi</a></td>
    </tr>
    <tr>
      <td><b>5b</b></td>
      <td><a href="chapter_5/section_5b/">Final AWS Deployment with Terraform & Docker</a></td>
    </tr>
  </tbody>
</table>

---

## 🙋 About

Created by **David Yu** – sharing practical backend engineering and infrastructure lessons through real-world builds.
