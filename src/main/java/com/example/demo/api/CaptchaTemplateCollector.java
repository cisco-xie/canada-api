package com.example.demo.api;

import lombok.extern.slf4j.Slf4j;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.util.*;

@Slf4j
public class CaptchaTemplateCollector {

    private static final String INPUT_DIR = "D:/captcha/";
    private static final String OUTPUT_DIR = "D:/captcha/templates/";
    private static final int TEMPLATE_WIDTH = 30;
    private static final int TEMPLATE_HEIGHT = 40;

    public static void main(String[] args) throws Exception {
        File dir = new File(INPUT_DIR);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".png"));
        if (files == null || files.length == 0) {
            log.error("目录中没有图片: {}", INPUT_DIR);
            return;
        }

        for (File file : files) {
            processImage(file);
        }
    }

    private static void processImage(File file) throws Exception {
        log.info("处理图片: {}", file.getName());
        BufferedImage image = ImageIO.read(file);
        if (image == null) {
            log.warn("读取图片失败: {}", file.getAbsolutePath());
            return;
        }

        // 灰度化
        BufferedImage gray = toGray(image);

        // 二值化
        BufferedImage binary = toBinary(gray);

        // 按列投影切割数字
        int[][] projection = getColumnProjection(binary);
        int[][] bounds = getDigitBounds(projection, binary.getHeight());

        // 遍历每个数字区域
        for (int i = 0; i < bounds.length; i++) {
            int xStart = bounds[i][0];
            int xEnd = bounds[i][1];

            BufferedImage digitImage = binary.getSubimage(xStart, 0, xEnd - xStart, binary.getHeight());
            BufferedImage resized = resize(digitImage, TEMPLATE_WIDTH, TEMPLATE_HEIGHT);

            // 人工标注
            int number = askHuman(file.getName(), i, bounds.length);

            // 保存模板
            saveTemplate(number, resized);
        }
    }

    /** 灰度化 */
    private static BufferedImage toGray(BufferedImage src) {
        BufferedImage gray = new BufferedImage(src.getWidth(), src.getHeight(), BufferedImage.TYPE_BYTE_GRAY);
        Graphics g = gray.getGraphics();
        g.drawImage(src, 0, 0, null);
        g.dispose();
        return gray;
    }

    /** 简单二值化 */
    private static BufferedImage toBinary(BufferedImage gray) {
        BufferedImage binary = new BufferedImage(gray.getWidth(), gray.getHeight(), BufferedImage.TYPE_BYTE_BINARY);
        Graphics g = binary.getGraphics();
        g.drawImage(gray, 0, 0, null);
        g.dispose();
        return binary;
    }

    /** 列投影: 每列黑色像素计数 */
    private static int[][] getColumnProjection(BufferedImage binary) {
        int w = binary.getWidth();
        int h = binary.getHeight();
        int[] projection = new int[w];

        for (int x = 0; x < w; x++) {
            int count = 0;
            for (int y = 0; y < h; y++) {
                int rgb = binary.getRGB(x, y) & 0xFF;
                if (rgb == 0) count++; // 黑色像素
            }
            projection[x] = count;
        }

        return new int[][]{projection};
    }

    /** 根据列投影获取数字左右边界 */
    private static int[][] getDigitBounds(int[][] projectionArr, int height) {
        int[] projection = projectionArr[0];
        int w = projection.length;
        java.util.List<int[]> boundsList = new java.util.ArrayList<>();

        boolean inDigit = false;
        int start = 0;

        for (int x = 0; x < w; x++) {
            if (!inDigit && projection[x] > height * 0.05) { // 阈值: 黑色像素>5%
                start = x;
                inDigit = true;
            } else if (inDigit && projection[x] <= height * 0.05) {
                boundsList.add(new int[]{start, x});
                inDigit = false;
            }
        }
        if (inDigit) boundsList.add(new int[]{start, w});

        return boundsList.toArray(new int[0][]);
    }

    /** 调整大小 */
    private static BufferedImage resize(BufferedImage src, int width, int height) {
        BufferedImage resized = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_BINARY);
        Graphics2D g2 = resized.createGraphics();
        g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
        g2.drawImage(src, 0, 0, width, height, null);
        g2.dispose();
        return resized;
    }

    /** 保存模板 */
    private static void saveTemplate(int number, BufferedImage digit) throws Exception {
        File dir = new File(OUTPUT_DIR + number);
        if (!dir.exists()) dir.mkdirs();
        String fileName = OUTPUT_DIR + number + "/" + System.currentTimeMillis() + ".png";
        try (FileOutputStream fos = new FileOutputStream(fileName)) {
            ImageIO.write(digit, "png", fos);
        }
        log.info("保存模板: {} -> {}", number, fileName);
    }

    /** 人工确认 */
    private static int askHuman(String fileName, int index, int total) {
        System.out.printf("图片: %s (%d/%d)，请输入数字(0-9):%n", fileName, index + 1, total);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            try {
                int n = scanner.nextInt();
                if (n >= 0 && n <= 9) return n;
            } catch (Exception ignored) {
            }
            System.out.println("请输入 0-9 的数字:");
            scanner.nextLine();
        }
    }
}