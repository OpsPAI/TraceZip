package org.example.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Random;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

public class MongoDB {

    private static final int NUM_DATABASES = 5;
    private static final int COLLECTIONS_PER_DB = 5;
    private static final int NUM_NAMES = 10000;

    public static void main(String[] args) throws InterruptedException {
        // 连接到 MongoDB 服务器
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");

        // 创建数据库和集合名称
        List<String> databases = new ArrayList<>();
        List<String> collections = new ArrayList<>();
        for (int i = 0; i < NUM_DATABASES; i++) {
            databases.add("testdb" + i);
            for (int j = 0; j < COLLECTIONS_PER_DB; j++) {
                collections.add("testcollection" + (i * COLLECTIONS_PER_DB + j));
            }
        }

        // 创建人名
        String[] names = new String[NUM_NAMES];
        for (int i = 0; i < NUM_NAMES; i++) {
            names[i] = "Person" + i;
        }

        Random random = new Random();

        for (int i = 0; i < 1000000; i++) {
            int operation = random.nextInt(5); // 0: 插入, 1: 查询, 2: 删除, 3: 更新, 4: 查询并更新
            String name = names[random.nextInt(names.length)];
            String dbName = databases.get((((name.hashCode()) % NUM_DATABASES) + NUM_DATABASES) % NUM_DATABASES);
            String collectionName = collections.get(((name.hashCode() / NUM_DATABASES) % collections.size() + collections.size())%collections.size() );

            // 访问数据库
            MongoDatabase database = mongoClient.getDatabase(dbName);

            // 访问集合
            MongoCollection<Document> collection = database.getCollection(collectionName);

            switch (operation) {
                case 0:
                    // 插入文档
                    Document insertDoc = new Document("name", name)
                            .append("age", random.nextInt(100))
                            .append("city", "City" + random.nextInt(100));
                    collection.insertOne(insertDoc);
                    System.out.println("Inserted: " + insertDoc.toJson());
                    break;
                case 1:
                    // 查询文档
                    Document queryDoc = collection.find(eq("name", name)).first();
                    if (queryDoc != null) {
                        System.out.println("Found: " + queryDoc.toJson());
                    } else {
                        System.out.println("No document found with name: " + name);
                    }
                    break;
                case 2:
                    // 删除文档
                    Document deleteDoc = collection.findOneAndDelete(eq("name", name));
                    if (deleteDoc != null) {
                        System.out.println("Deleted: " + deleteDoc.toJson());
                    } else {
                        System.out.println("No document found to delete with name: " + name);
                    }
                    break;
                case 3:
                    // 更新文档
                    Document updateDoc = collection.findOneAndUpdate(eq("name", name), set("age", random.nextInt(100)));
                    if (updateDoc != null) {
                        System.out.println("Updated: " + updateDoc.toJson());
                    } else {
                        System.out.println("No document found to update with name: " + name);
                    }
                    break;
                case 4:
                    // 查询并更新文档
                    Document queryAndUpdateDoc = collection.findOneAndUpdate(eq("name", name), set("city", "City" + random.nextInt(100)));
                    if (queryAndUpdateDoc != null) {
                        System.out.println("Found and Updated: " + queryAndUpdateDoc.toJson());
                    } else {
                        System.out.println("No document found to update with name: " + name);
                    }
                    break;
            }
        }

        // 关闭连接
        mongoClient.close();
    }
}
