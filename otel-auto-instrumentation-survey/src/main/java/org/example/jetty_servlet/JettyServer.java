package org.example.jetty_servlet;


import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class JettyServer {

    public static void main(String[] args) throws Exception {
        // 创建 Jetty 服务器
        Server server = new Server(9070);

        // 创建 Servlet 上下文处理器
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        // 添加 Servlet 到上下文
        context.addServlet(new ServletHolder(new HelloServlet()), "/1a3fef35-bfb8-440c-b8b8-eb57a3f01d2a");
        context.addServlet(new ServletHolder(new HelloServlet()), "/ca254a0d-786d-4331-becf-37a35d713253");
        context.addServlet(new ServletHolder(new HelloServlet()), "/7ed3f15b-23e6-48c6-8eb4-65f4790dacc6");
        context.addServlet(new ServletHolder(new HelloServlet()), "/2bddf016-7afa-4473-aa15-1d7f917e5ad4");

        // 设置服务器的处理器
        server.setHandler(context);

        // 启动服务器
        server.start();
        System.out.println("Jetty server started.");

        // 等待服务器终止
        server.join();
    }
}