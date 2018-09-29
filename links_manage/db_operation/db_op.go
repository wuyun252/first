package linkdb

import (
	"common"
	"github.com/golang/glog"
	"fmt"
)


type DnsCoverLinkIdInfo struct {
	NationId   int64
	ProvinceId int64
	IspId      int64
	IdcId    int64
}


type DnsCoverLinkNameInfo struct {
	NationName   string `form:"nation" json:"nation" binding:"omitempty"`
	ProvinceName string `form:"province" json:"province" binding:"required"`
	IspName      string `form:"isp" json:"isp" binding:"required"`
	IdcName      string `form:"idc_name" json:"idc_name" binding:"required"`
}


func (linkIdInfo *DnsCoverLinkIdInfo)  TransformToNameInfo(geoInfo *common.GeoInfo, idcId2Name map[int64]string) (DnsCoverLinkNameInfo, error) {
	var result DnsCoverLinkNameInfo
	nationName, ok := geoInfo.NationId2Name[linkIdInfo.NationId]
	if !ok {
		glog.Warningf("nation id %s has no responding nation name", linkIdInfo.NationId)
		return result, fmt.Errorf("nation id %s has no responding nation name", linkIdInfo.NationId)
	}
	provinceName, ok := geoInfo.ProId2Name[linkIdInfo.ProvinceId]
	if !ok {
		glog.Warningf("pro id %s has no responding pro name", linkIdInfo.ProvinceId)
		return result, fmt.Errorf("pro id %s has no responding pro name", linkIdInfo.ProvinceId)
	}
	ispName, ok := geoInfo.IspId2Name[linkIdInfo.IspId]
	if !ok {
		glog.Warningf("isp id %s has no responding isp name", linkIdInfo.IspId)
		return result, fmt.Errorf("isp id %s has no responding isp name", linkIdInfo.IspId)
	}
	idcName, ok := idcId2Name[linkIdInfo.IdcId]
	if !ok {
		glog.Warningf("idc id %s has no responding idc name", linkIdInfo.IdcId)
		return result, fmt.Errorf("idc id %s has no responding idc name", linkIdInfo.IdcId)
	}
	result.IdcName = idcName
	result.IspName = ispName
	result.ProvinceName = provinceName
	result.NationName = nationName
	return result, nil
}

func (linkIdInfo *DnsCoverLinkNameInfo)  TransformToIdInfo(geoInfo *common.GeoInfo, idcName2Id map[string]int64) (DnsCoverLinkIdInfo, error) {
	var result DnsCoverLinkIdInfo
	nationId, ok := geoInfo.NationName2Id[linkIdInfo.NationName]
	if !ok {
		glog.Warningf("nation name %s has no responding nation id", linkIdInfo.NationName)
		return result, fmt.Errorf("nation name %s has no responding nation id", linkIdInfo.NationName)
	}
	provinceId, ok := geoInfo.ProName2Id[linkIdInfo.ProvinceName]
	if !ok {
		glog.Warningf("pro name %s has no responding pro id", linkIdInfo.ProvinceName)
		return result, fmt.Errorf("pro name %s has no responding pro id", linkIdInfo.ProvinceName)
	}
	ispId, ok := geoInfo.IspName2Id[linkIdInfo.IspName]
	if !ok {
		glog.Warningf("isp name %s has no responding isp id", linkIdInfo.IspName)
		return result, fmt.Errorf("isp name %s has no responding isp id", linkIdInfo.IspName)
	}
	idcId, ok := idcName2Id[linkIdInfo.IdcName]
	if !ok {
		glog.Warningf("idc name %s has no responding idc id", linkIdInfo.IdcName)
		return result, fmt.Errorf("idc name %s has no responding idc id", linkIdInfo.IdcName)
	}
	result.IdcId = idcId
	result.IspId = ispId
	result.ProvinceId = provinceId
	result.NationId = nationId
	return result, nil
}


type PostLink struct {
	DnsCoverLinkNameInfo
	ExceptionMask int64 `json:"exception_mask"`
}

type PostLinkId struct {
	DnsCoverLinkIdInfo
	ExceptionMask int64 `json:"exception_mask"`
}

func GetConcernedClusterIds(dbHelper *common.DBHelper) (map[int64]bool, error) {
	sqlPrefix := "SELECT cluster_id FROM link_detect_cluster"
	rows, err := dbHelper.Query(sqlPrefix)

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[int64]bool)
	for rows.Next() {
		var clusterId int64
		err = rows.Scan(&clusterId)
		if err != nil {
			return nil, err
		}
		result[clusterId] = true
	}
	return result, nil
}

func UpdateLinkMask(dbHelper *common.DBHelper, postLinks []PostLinkId) (int64, error) {
	sqlPreix := "INSERT INTO link_detect_info(nation_id, province_id, isp_id, idc_id, exception_mask, ctime) VALUES"
	var vals []interface{}
	placeHold := "(?, ?, ?, ?, ?, NOW())"
	for _, link := range postLinks {
		vals = append(vals, link.NationId, link.ProvinceId, link.IspId, link.IdcId, link.ExceptionMask)
	}
	sqlPostfix := " ON DUPLICATE KEY UPDATE exception_mask=VALUES(exception_mask), ctime=NOW()"
	return dbHelper.InsertBatch(sqlPreix, placeHold, sqlPostfix, len(postLinks), vals...)
}

func DeleteRestoredLink(dbHelper *common.DBHelper) (int64, error) {
	return dbHelper.Delete("DELETE FROM link_detect_info WHERE exception_mask=0")
}

type OssDb struct {
	Master string
	Slaver string
}

func GetClusterOssIps(dbHelper *common.DBHelper) ([]OssDb, error) {
	needClusterIds, err := GetConcernedClusterIds(dbHelper)
	if err != nil {
		glog.Errorf("get cluster ids need has detect links failed  %s", err.Error())
		return nil, err
	}
	queryString := "SELECT id, db, slave_db FROM cluster_info"
	rows, err := dbHelper.Query(queryString)
	if err != nil {
		glog.Errorf("get cluster db info failed for %s", err.Error())
		return nil, err
	}
	defer rows.Close()
	var result []OssDb
	for rows.Next() {
		var cid int64
		var master, slaver string
		err = rows.Scan(&cid, &master, &slaver)
		if err != nil {
			glog.Errorf("scan db result failed: %s ", err.Error())
			return nil, err
		}
		if len(master) == 0 {
			glog.Warning("some cluster has no db data")
		} else if !needClusterIds[cid] {
			glog.Infof("the cluster id %d need not to touch", cid)
		} else {
			result = append(result, OssDb{Master: master, Slaver: slaver})
		}
	}
	if err = rows.Err(); err != nil {
		glog.Errorf("some error happends in db scan: %s", err.Error())
		return nil, err
	}
	return result, nil
}
