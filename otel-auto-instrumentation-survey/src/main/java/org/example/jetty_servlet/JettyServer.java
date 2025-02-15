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
        context.addServlet(new ServletHolder(new HelloServlet()), "/hello1");
        context.addServlet(new ServletHolder(new HelloServlet()), "/hello2");
        context.addServlet(new ServletHolder(new HelloServlet()), "/hello3");
        context.addServlet(new ServletHolder(new HelloServlet()), "/hello4");

        // 设置服务器的处理器
        server.setHandler(context);

        // 启动服务器
        server.start();
        System.out.println("Jetty server started.");

        // 等待服务器终止
        server.join();
    }
}