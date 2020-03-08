package com.lsd.etl.itag.controller;

import com.lsd.etl.itag.dto.TagDto;
import com.lsd.etl.itag.es.EsMemberTagETL;
import com.lsd.etl.itag.service.EsService;
import com.lsd.etl.itag.vo.MemberTag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Created by lsd
 * 2020-03-06 20:49
 */
@Slf4j
@Controller
public class EsQueryController {

    @Autowired
    private EsService esService;
    @Autowired
    private EsMemberTagETL esMemberTagETL;

    /**
     * 用户数据清洗并存入ES中
     */
    @RequestMapping("/index")
    @ResponseBody
    public String etlAndIndex() {
        esMemberTagETL.etlAndIndex();
        return "success";
    }


    /**
     * 查询并生成文本文件
     */
    @RequestMapping("/gen")
    public void queryAndGen(@RequestBody List<TagDto> tagList, HttpServletResponse response) {
        final List<MemberTag> memberTags = esService.query(tagList);
        String txtFileContent = totxtFileContent(memberTags);
        try (
                ServletOutputStream os = response.getOutputStream();
                BufferedOutputStream bos = new BufferedOutputStream(os);
        ) {
            response.setHeader("Content-Disposition", "attachment; filename=" + URLEncoder.encode("会员查询结果.txt", "UTF-8"));
            bos.write(txtFileContent.getBytes(StandardCharsets.UTF_8));
            bos.flush();
        } catch (Exception e) {
            log.error("导出文本文件失败", e);
        }
    }


    /**
     * 结果转为文本，把 List<MemberTag> -> String
     */
    private String totxtFileContent(List<MemberTag> memberTags) {
        StringBuilder sb = new StringBuilder();
        for (MemberTag tag : memberTags) {
            sb.append("[")
                    .append(tag.getMemberId()).append(",").append(tag.getPhone())
                    .append("]\r\n");
        }
        return sb.toString();
    }

}
